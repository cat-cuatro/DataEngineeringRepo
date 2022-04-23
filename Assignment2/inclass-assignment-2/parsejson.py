import json

def main():
    f = open('bcsample.json')
    data = json.load(f)
    for i in data:
        print(i)

if __name__ == "__main__":
    main()
