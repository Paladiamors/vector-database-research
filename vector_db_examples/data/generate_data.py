import json
import numpy as np
import os
from sentence_transformers import SentenceTransformer

def generate_dataset():
    model = SentenceTransformer('all-MiniLM-L6-v2')

    sentences = [
        {"text": "Apple is a popular fruit.", "category": "fruit"},
        {"text": "Bananas are rich in potassium.", "category": "fruit"},
        {"text": "The quick brown fox jumps over the lazy dog.", "category": "animal"},
        {"text": "Machine learning is a subset of artificial intelligence.", "category": "tech"},
        {"text": "Deep learning models require large datasets.", "category": "tech"},
        {"text": "Docker simplifies application deployment.", "category": "tech"},
        {"text": "Renaissance art is characterized by realism.", "category": "art"},
        {"text": "The Mona Lisa was painted by Leonardo da Vinci.", "category": "art"},
        {"text": "Vectors are mathematical objects with magnitude and direction.", "category": "math"},
        {"text": "Linear algebra is essential for understanding vector spaces.", "category": "math"}
    ]

    embeddings = model.encode([item["text"] for item in sentences])

    dataset = []
    for i, (item, embedding) in enumerate(zip(sentences, embeddings)):
        dataset.append({
            "id": i + 1,
            "text": item["text"],
            "metadata": {"category": item["category"]},
            "vector": embedding.tolist()
        })

    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_path = os.path.join(script_dir, 'dataset.json')

    with open(output_path, 'w') as f:
        json.dump(dataset, f, indent=2)

    print(f"Generated dataset with {len(dataset)} items in {output_path}")

if __name__ == "__main__":
    generate_dataset()
