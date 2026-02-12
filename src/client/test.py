import os

from tinkoff.invest.sandbox.client import SandboxClient

TOKEN = os.getenv('INVEST_TOKEN')

def main():
    with SandboxClient(TOKEN) as client:
        r = client.instruments.find_instrument(query="SBER")
        for i in r.instruments:
            print(i)


if __name__ == "__main__":
    main()

