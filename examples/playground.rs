use std::time::Duration;
use tokio::select;
use tokio::time::sleep;

#[tokio::main]
pub async fn main() -> anyhow::Result<()> {
    select! {
        _ = {
            loop {
                println!("a");
                sleep(Duration::from_millis(1000)).await;
            }
            sleep(Duration::from_millis(1))
        } => {}
        _ = {
            loop {
                println!("b");
                sleep(Duration::from_millis(2000)).await;
            }
            sleep(Duration::from_millis(1))
        } => {}

        // _ = asdf("a", 1000) => {}
        // _ = asdf("b", 2000) => {}
    }



    Ok(())
}

async fn asdf(name: &str, millis: u64) {
    loop {
        println!("{}", name);
        sleep(Duration::from_millis(millis)).await;
    }
}