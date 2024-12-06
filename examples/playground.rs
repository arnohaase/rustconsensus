use std::any::Any;

trait Message: Any {
    // fn as_any(&self) -> &dyn Any;
}

impl<T: Any> Message for T {
    // fn as_any(&self) -> &dyn Any {
    //     self
    // }
}

struct MyMessage {
    content: String,
}

impl MyMessage {
    fn new(content: &str) -> Self {
        MyMessage {
            content: content.to_string(),
        }
    }
}

fn main() {
    // Create a Vec<Box<dyn Message>>
    let mut messages: Vec<Box<dyn Message>> = Vec::new();

    messages.push(Box::new(MyMessage::new("Hello")));
    messages.push(Box::new(MyMessage::new("World")));

    // let comparison_message = MyMessage::new("Hello");

    // let first_message = messages.remove(0);
    // if let Ok(cast_message) = first_message.downcast::<MyMessage>() {
    //
    // }


        // if let Some(my_message) = first_message.as_any().downcast_ref::<Box<MyMessage>>() {
        //     if my_message.content == comparison_message.content {
        //         println!("The messages are equal!");
        //     } else {
        //         println!("The messages are different.");
        //     }
        // } else {
        //     println!("The first message is not of type MyMessage.");
        // }
    // } else {
    //     println!("The Vec is empty.");
    // }
}
