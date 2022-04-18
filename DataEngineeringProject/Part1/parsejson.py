import json

def main():
    f = open('bcsample.json')
    data = json.load(f)
    for i in data:
        temp = json.dumps(i)
        print('New Entry of type:', type(temp),':', temp)

if __name__ == "__main__":
    main()
