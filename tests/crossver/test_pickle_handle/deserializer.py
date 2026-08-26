import pickle

if __name__ == "__main__":
    with open("pickle.handle", "rb") as f:
        pickle.load(f)
