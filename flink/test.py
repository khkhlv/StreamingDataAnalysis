from pyflink.datastream import StreamExecutionEnvironment
import json

def run():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    
    ds = env.from_collection([
        json.dumps({"ticker": "TEST", "price": 100.0}),
        json.dumps({"ticker": "TEST", "price": 101.5})
    ])
    
    ds.map(lambda x: json.loads(x)["price"] * 1.01).print()
    
    env.execute("Test Job")

if __name__ == "__main__":
    run()