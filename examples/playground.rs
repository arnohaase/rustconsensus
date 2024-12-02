use std::marker::PhantomPinned;

pub fn main() {
    // let a = Rc::new(MyStruct::default());
    // {
    //     let p = Pin::new(a);
    //     println!("pinned: {:?}", p);
    // }
    // println!("pin is out of scope: {:?}", a);
}

#[derive(Default, Debug)]
struct MyStruct {
    a: PhantomPinned,
}
impl Drop for MyStruct {
    fn drop(&mut self) {
        println!("dropping MyStruct");
    }
}