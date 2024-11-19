use tokio_uring::{
    buf::{IoBuf, IoBufMut},
    fs::File,
    ReadData, ReadTransform, UnsubmittedRead, UnsubmittedWrite, WriteData, WriteTransform,
};

enum Inst<R: IoBufMut, W: IoBuf> {
    Write(File, W),
    Read(File, R),
}

enum UnsubmittedOneshot<R: IoBufMut, W: IoBuf> {
    Read(tokio_uring::UnsubmittedOneshot<ReadData<R>, ReadTransform<R>>),
    Write(tokio_uring::UnsubmittedOneshot<WriteData<W>, WriteTransform<W>>),
}

enum InflightOneshot<R: IoBufMut, W: IoBuf> {
    Read(tokio_uring::InFlightOneshot<ReadData<R>, ReadTransform<R>>),
    Write(tokio_uring::InFlightOneshot<WriteData<W>, WriteTransform<W>>),
}

struct Program<R: IoBufMut, W: IoBuf> {
    insts: Vec<Inst<R, W>>,
}

impl<R: IoBufMut, W: IoBuf> Program<R, W> {
    fn execute(self) -> Vec<InflightOneshot<R, W>> {
        let mut inflights = Vec::new();
        for inst in self.insts {
            match inst {
                Inst::Write(file, buf) => {
                    let inflight = file.write_at(buf, 0).submit();
                    inflights.push(InflightOneshot::Write(inflight));
                }
                Inst::Read(file, buf) => {
                    let inflight = file.unsubmitted_read_at(buf, 0).submit();
                    inflights.push(InflightOneshot::Read(inflight));
                }
            }
        }
        inflights
    }
}

#[derive(Default)]
struct ProgramBuilder<R: IoBufMut, W: IoBuf> {
    insts: Vec<Inst<R, W>>,
}

impl<R: IoBufMut, W: IoBuf> ProgramBuilder<R, W> {
    fn write(mut self, file: File, buffer: W) -> Self {
        self.insts.push(Inst::Write(file, buffer));
        self
    }

    fn read(mut self, file: File, buffer: R) -> Self {
        self.insts.push(Inst::Read(file, buffer));
        self
    }

    fn build(self) -> Program<R, W> {
        Program { insts: self.insts }
    }
}

macro_rules! build_program {
	($first:expr, $($rest:expr),+) => {{
        let linked = $first.set_link();
        let tail = build_program!($($rest),+);
        (linked, (tail))
    }};
    ($last:expr) => {{
        ($last, ())
    }};
}

trait Submittable {
    type Output;
    fn submit(self) -> Self::Output;
}

impl<B: IoBufMut> Submittable for UnsubmittedRead<B> {
    type Output = tokio_uring::InFlightOneshot<ReadData<B>, ReadTransform<B>>;
    fn submit(self) -> Self::Output {
        self.submit()
    }
}

impl<B: IoBuf> Submittable for UnsubmittedWrite<B> {
    type Output = tokio_uring::InFlightOneshot<WriteData<B>, WriteTransform<B>>;
    fn submit(self) -> Self::Output {
        self.submit()
    }
}

trait SubmittableVariadic {
    type Output;
    fn submit_all(self) -> Self::Output;
}

impl SubmittableVariadic for () {
    type Output = ();
    fn submit_all(self) -> Self::Output {
        ()
    }
}

impl<F: Submittable, Rest: SubmittableVariadic> SubmittableVariadic for (F, Rest) {
    type Output = (F::Output, Rest::Output);
    fn submit_all(self) -> Self::Output {
        let inflight = self.0.submit();
        let inflight_right = self.1.submit_all();
        (inflight, inflight_right)
    }
}

async fn old_ver() {
    let file = File::create("asdf").await.unwrap();
    let buf = Vec::from([7u8; 10]);
    // let submitted_write1 = fs.write_at(buf, 0).submit();

    let program: Program<Vec<u8>, Vec<u8>> = ProgramBuilder::default().write(file, buf).build();
    let inflights = program.execute();
    for inflight in inflights {
        match inflight {
            InflightOneshot::Read(read) => {
                let (res, buf) = read.await;
                res.unwrap();
            }
            InflightOneshot::Write(write) => {
                let (res, buf) = write.await;
                res.unwrap();
            }
        }
    }
}

fn main() {
    // let tempfile = tempfile::NamedTempFile::new().unwrap();
    tokio_uring::start(async {
        let file = File::create("asdf").await.unwrap();
        let buf = Vec::from([7u8; 10]);
        let unsubmitted1 = file.unsubmitted_read_at(buf, 0);

        let file = File::create("asdf").await.unwrap();
        let buf = Vec::from([7u8; 10]);
        let unsubmitted2 = file.unsubmitted_read_at(buf, 0);

        let unsubmitted_program = build_program!(unsubmitted1, unsubmitted2);
        // (UnsubmittedRead<Vec<u8>>, (UnsubmittedRead<Vec<u8>>, ()))

        let inflights = unsubmitted_program.submit_all();

        let (inflight1, inflights) = inflights;
        let result1 = inflight1.await;

        let (inflight2, inflights) = inflights;
        let result2 = inflight2.await;

        // comple error
        // It is guaranteed at compile time to have a length of 2.
        // let (inflight2, inflights) = inflights;
    });
}
