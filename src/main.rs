use chrono::Utc;
use dotenv::dotenv;
use rppal::i2c::I2c;
use std::env;
use std::time::Duration;
use tokio;
use tokio::time::sleep;
use tokio_postgres::{Error, NoTls};

#[tokio::main]
async fn main() -> Result<(), Error> {
    dotenv().ok();

    // Read the SENSOR_ID from environment variables
    let sensor_id = env::var("SENSOR_ID").expect("SENSOR_ID must be set");

    // Read the database connection parameters from environment variables
    let host = env::var("DB_HOST").expect("DB_HOST must be set");
    let user = env::var("DB_USER").expect("DB_USER must be set");
    let password = env::var("DB_PASSWORD").expect("DB_PASSWORD must be set");
    let dbname = env::var("DB_NAME").expect("DB_NAME must be set");

    // Set up the database connection
    let (client, connection) = tokio_postgres::connect(
        &format!("host={} user={} password={} dbname={}", host, user, password, dbname),
        NoTls,
    )
    .await?;

    // The connection object performs the actual communication with the database,
    // so spawn it off to run on its own.
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    // Initialize the I2C bus
    let mut i2c = I2c::new().unwrap();
    i2c.set_slave_address(0x44).unwrap(); // SHT45 default I2C address

    loop {
        // Read data from the sensor
        let data = read_sht45(&mut i2c).unwrap();

        // Convert the current time to a string
        let current_time: chrono::DateTime<Utc> = Utc::now();
        let time_str = current_time.to_rfc3339();

        // Insert data into the database
        client.execute(
            "INSERT INTO sensor_data (sensor_id, temperature, humidity, time) VALUES ($1, $2, $3, $4)",
            &[&sensor_id, &data.temperature, &data.humidity, &time_str],
        ).await?;

        println!(
            "Inserted data: sensor_id={}, temperature={}, humidity={}",
            sensor_id, data.temperature, data.humidity
        );

        // Sleep for a minute before the next reading
        sleep(Duration::from_secs(60)).await;
    }
}

struct SensorData {
    temperature: f64,
    humidity: f64,
}

fn read_sht45(i2c: &mut I2c) -> Result<SensorData, Box<dyn std::error::Error>> {
    let mut buf = [0u8; 6];

    // Send measurement command
    i2c.write(&[0xFD])?;
    std::thread::sleep(Duration::from_millis(10)); // Wait for the measurement to complete
    i2c.read(&mut buf)?;

    // Convert bytes to temperature and humidity
    let raw_temp = ((buf[0] as u16) << 8) | buf[1] as u16;
    let raw_hum = ((buf[3] as u16) << 8) | buf[4] as u16;

    let temperature = -45.0 + 175.0 * (raw_temp as f64 / 65535.0);
    let humidity = 100.0 * (raw_hum as f64 / 65535.0);

    Ok(SensorData {
        temperature,
        humidity,
    })
}
