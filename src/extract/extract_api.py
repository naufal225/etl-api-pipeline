import requests
import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[2]
BASE_DIR.mkdir(exist_ok=True)

def fetch_json(url: str):
    r = requests.get(url=url, timeout=10)
    r.raise_for_status()
    return r

def save_json(data, path: Path):
    df = pd.DataFrame(data)
    df.to_json(path, orient="records")
    
def extract_all():
    users = fetch_json("https://jsonplaceholder.typicode.com/users")
    posts = fetch_json("https://jsonplaceholder.typicode.com/posts")
    comments = fetch_json("https://jsonplaceholder.typicode.com/comments")
    
    save_json(users, BASE_DIR / "users.json")
    save_json(posts, BASE_DIR / "posts.json")
    save_json(comments, BASE_DIR / "comments.json")
    
if __name__ == "__main__":
    extract_all()