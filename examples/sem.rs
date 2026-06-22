use monitor::{
    semaphores::{Process, Semaphore},
    seq,
};

pub fn sequence() {
    let join_6_7 = Semaphore::new(0); // needs s6 and s7
    let join_8_9 = Semaphore::new(0); // needs s8 and s9
    let join_2_3 = Semaphore::new(0); // needs s2 and s3
    let join_5_6 = Semaphore::new(0); // needs s5 and s6
    let join_3_4 = Semaphore::new(0); // needs s3 and s4

    let c0 = Semaphore::new(1);
    let c1 = Semaphore::new(0);
    let c2 = Semaphore::new(0);
    let c3 = Semaphore::new(0);
    let c4 = Semaphore::new(0);
    let c7 = Semaphore::new(0);

    let s0 = Process::new("s0").wait_on(&c0).release_on(&c1);
    let s1 = Process::new("s1")
        .wait_on(&c1)
        .release_on_many_borrowed(&[&c2, &c3, &c4]);
    let s2 = Process::new("s2").wait_on(&c2).release_on(&join_2_3);
    let s3 = Process::new("s3")
        .wait_on(&c3)
        .release_on_many_borrowed(&[&join_2_3, &join_3_4]);
    let s4 = Process::new("s4")
        .wait_on(&c4)
        .release_on_many_borrowed(&[&join_3_4, &c7]);
    let s5 = Process::new("s5")
        .wait_on_many_borrowed(&[&join_2_3, &join_2_3])
        .release_on(&join_5_6);
    let s6 = Process::new("s6")
        .wait_on_many_borrowed(&[&join_3_4, &join_3_4])
        .release_on_many_borrowed(&[&join_5_6, &join_6_7]);
    let s7 = Process::new("s7").wait_on(&c7).release_on(&join_6_7);
    let s8 = Process::new("s8")
        .wait_on_many_borrowed(&[&join_5_6, &join_5_6])
        .release_on(&join_8_9);
    let s9 = Process::new("s9")
        .wait_on_many_borrowed(&[&join_6_7, &join_6_7])
        .release_on(&join_8_9);
    let sa = Process::new("sa")
        .wait_on_many_borrowed(&[&join_8_9, &join_8_9])
        .release_on(&c0);

    let sequence = seq![s8, s1, s2, s3, sa, s5, s6, s7, s9, s4, s0];

    sequence.run();
}

fn main() {
    sequence();
}
