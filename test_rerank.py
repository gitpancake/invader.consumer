import asyncio
import sys
sys.path.insert(0, 'src')

from encoder.tile_rerank import rerank_with_tiles
from encoder.grid_detect import preprocess_with_grid_detection
from PIL import Image
import httpx
from io import BytesIO

async def test_rerank():
    # Download PA_1318 test image
    url = "https://www.space-invaders.com/wp-content/uploads/2022/02/PA-1318-288x300.jpg"
    async with httpx.AsyncClient() as client:
        resp = await client.get(url)
        img_data = resp.content

    query_img = Image.open(BytesIO(img_data))

    # Apply grid detection first
    processed = preprocess_with_grid_detection(query_img)

    # Simulate top 10 CLIP candidates (from previous test results)
    # PA_1318 was at position 4 with similarity 0.7097
    candidates = [
        {"flash_id": 1, "flash_name": "PA_1086", "similarity": 0.7299},
        {"flash_id": 2, "flash_name": "PA_1287", "similarity": 0.7158},
        {"flash_id": 3, "flash_name": "PA_1385", "similarity": 0.7134},
        {"flash_id": 4, "flash_name": "PA_1318", "similarity": 0.7097},  # Target
        {"flash_id": 5, "flash_name": "PA_1329", "similarity": 0.7054},
        {"flash_id": 6, "flash_name": "PA_1362", "similarity": 0.7019},
        {"flash_id": 7, "flash_name": "PA_1295", "similarity": 0.6998},
        {"flash_id": 8, "flash_name": "PA_1340", "similarity": 0.6975},
        {"flash_id": 9, "flash_name": "PA_1280", "similarity": 0.6952},
        {"flash_id": 10, "flash_name": "PA_1300", "similarity": 0.6930},
    ]

    print("Original CLIP rankings:")
    for i, c in enumerate(candidates, 1):
        marker = "<-- TARGET" if c["flash_name"] == "PA_1318" else ""
        print(f"  {i}. {c['flash_name']} - {c['similarity']:.4f} {marker}")

    print("\nRe-ranking with tile patterns...")
    reranked = await rerank_with_tiles(processed, candidates, top_k=10, tile_weight=0.4)

    print("\nAfter tile re-ranking:")
    for i, c in enumerate(reranked, 1):
        marker = "<-- TARGET" if c["flash_name"] == "PA_1318" else ""
        tile_sim = c.get("tile_similarity", 0)
        combined = c.get("combined_score", c["similarity"])
        print(f"  {i}. {c['flash_name']} - combined: {combined:.4f} (emb: {c['similarity']:.4f}, tile: {tile_sim:.4f}) {marker}")

if __name__ == "__main__":
    asyncio.run(test_rerank())
