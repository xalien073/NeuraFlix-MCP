import os, httpx, tempfile, asyncio
from uuid import uuid4
from mcp.server.fastmcp import FastMCP
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient
from gremlin_python.driver.client import Client
from gremlin_python.driver.serializer import GraphSONSerializersV2d0

load_dotenv()
mcp = FastMCP("neuraflix-mcp")

OMDB_API_KEY = os.getenv("OMDB_API_KEY")
OMDB_BASE_URL = "http://www.omdbapi.com"

# Helper to run blocking functions in executor
def run_blocking(fn, *args, **kwargs):
    return asyncio.get_running_loop().run_in_executor(None, lambda: fn(*args, **kwargs))

async def fetch_omdb(title):
    params = {"t": title, "apikey": OMDB_API_KEY}
    async with httpx.AsyncClient(timeout=15) as client:
        try:
            resp = await client.get(OMDB_BASE_URL, params=params)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            print("[OMDb] Error:", e)
            return None

async def upload_blob(local_path):
    conn = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    container = os.getenv("AZURE_STORAGE_CONTAINER_NAME")
    def _upload():
        bs = BlobServiceClient.from_connection_string(conn)
        cc = bs.get_container_client(container)
        blob_name = f"{uuid4()}-{os.path.basename(local_path)}"
        blob = cc.get_blob_client(blob_name)
        with open(local_path, "rb") as f:
            blob.upload_blob(f, overwrite=True)
        return blob.url
    return await run_blocking(_upload)

async def gremlin_insert(movie_id, actual_title, year, genre, poster_url, directors, actors):
    endpoint = os.getenv("GREMLIN_ENDPOINT")
    db = os.getenv("GREMLIN_DB_NAME")
    graph = os.getenv("GREMLIN_COLLECTION")
    key = os.getenv("GREMLIN_PK")

    def _work():
        client = Client(endpoint, 'g',
                        username=f"/dbs/{db}/colls/{graph}",
                        password=key,
                        message_serializer=GraphSONSerializersV2d0())
        client.submit(f"""
          g.V().has('movie','id','{movie_id}').fold().coalesce(
            unfold(),
            addV('movie')
              .property('id','{movie_id}')
              .property('title','{actual_title}')
              .property('genre','{genre}')
              .property('year','{year}')
              .property('thumbnail','{poster_url}')
          )""")
        for d in directors:
            d_id = d.replace(" ", "_")
            client.submit(f"""
              g.V().has('director','id','{d_id}').fold().coalesce(
                unfold(),
                addV('director').property('id','{d_id}').property('name','{d}')
              )""")
            client.submit(f"""
              g.V().has('director','id','{d_id}')
                .addE('Directed')
                .to(g.V().has('movie','id','{movie_id}'))
            """)
        for a in actors:
            a_id = a.replace(" ", "_")
            client.submit(f"""
              g.V().has('actor','id','{a_id}').fold().coalesce(
                unfold(),
                addV('actor').property('id','{a_id}').property('name','{a}')
              )""")
            client.submit(f"""
              g.V().has('actor','id','{a_id}')
                .addE('ActedIn')
                .to(g.V().has('movie','id','{movie_id}'))
            """)
        client.close()
    await run_blocking(_work)

@mcp.tool()
async def insert_movie_with_details(title: str) -> str:
    """
    Insert a movie and its metadata into the NeuraFlix knowledge graph.

    This tool fetches movie details from the OMDb API using the given title,
    downloads and uploads the poster image to Azure Blob Storage, and inserts
    the movie, its directors, and actors into the Cosmos DB Gremlin graph database.
    
    Relationships added:
    - Movie vertex with properties: id, title, genre, year, thumbnail
    - Director vertex and 'Directed' edge to the movie
    - Actor vertex and 'ActedIn' edge from the movie

    Parameters:
    - title (str): The title of the movie to fetch and insert.

    Returns:
    - str: Status message indicating whether the operation succeeded or failed.
    """
    data = await fetch_omdb(title)
    if not data or data.get("Response")=="False":
        return f"Movie not found: {title}"

    at = data["Title"]
    year = data.get("Year","Unknown")
    genre = data.get("Genre","Unknown")
    poster = data.get("Poster")
    dirs = [d.strip() for d in data.get("Director","").split(",")]
    acts = [a.strip() for a in data.get("Actors","").split(",")]

    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".jpg")
    try:
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.get(poster)
            r.raise_for_status()
            tmp.write(r.content)
    except Exception as e:
        print("[Poster]", e)
        tmp.close()
        return "Metadata fetched, poster download failed."
    tmp.close()

    blob_url = await upload_blob(tmp.name)
    os.remove(tmp.name)
    if not blob_url:
        return "Metadata fetched, poster upload failed."

    movie_id = at.replace(" ","_")
    await gremlin_insert(movie_id, at, year, genre, blob_url, dirs, acts)

    return f"Inserted: {at}"

@mcp.resource("test://{msg}")
def test(msg:str)->str:
    return f"ok: {msg}"



# import os
# import aiohttp
# import shutil
# from uuid import uuid4
# from gremlin_python.driver.client import Client
# from azure.storage.blob.aio import BlobServiceClient
# from mcp.server.fastmcp import FastMCP
# from dotenv import load_dotenv

# load_dotenv()
# mcp = FastMCP("neuraflix-mcp")

# # Gremlin setup
# print("[Init] Setting up Gremlin client...")
# gremlin_client = Client(
#     os.getenv("GREMLIN_ENDPOINT"),
#     'g',
#     username=f"/dbs/{os.getenv('GREMLIN_DB_NAME')}/colls/{os.getenv('GREMLIN_CONTAINER')}",
#     password=os.getenv("GREMLIN_PK"),
#     message_serializer=None
# )

# # Blob setup
# print("[Init] Setting up Azure Blob client...")
# blob_service_client = BlobServiceClient.from_connection_string(os.getenv("AZURE_STORAGE_CONNECTION_STRING"))
# container_client = blob_service_client.get_container_client(os.getenv("AZURE_STORAGE_CONTAINER_NAME"))

# OMDB_API_KEY = os.getenv("OMDB_API_KEY")


# @mcp.tool()
# async def insert_movie_with_details(title: str) -> str:
#     """Given a movie title, fetch metadata, upload thumbnail, and insert into Gremlin DB."""
#     api_url = f"http://www.omdbapi.com/?t={title}&apikey={OMDB_API_KEY}"
#     print(f"[OMDb] Fetching data from: {api_url}")

#     async with aiohttp.ClientSession() as session:
#         async with session.get(api_url) as response:
#             data = await response.json()
#             print('data', data["Response"])

#     if data.get("Response") == "False":
#         print(f"[OMDb] Movie not found: {title}")
#         return f"Movie not found in OMDb: {title}"

#     print(f"[OMDB] Found: {data.get('Title')} ({data.get('Year')})")

#     actualTitle = data.get('Title')
#     year = data.get("Year", "Unknown")
#     genre = data.get("Genre", "Unknown")
#     director_list = [d.strip() for d in data.get("Director", "").split(",")]
#     actor_list = [a.strip() for a in data.get("Actors", "").split(",")]
#     poster_url = data.get("Poster")

#     # Download poster
#     local_filename = f"{actualTitle}.jpg"
#     print(f"[Poster] Downloading poster: {poster_url}")
#     async with aiohttp.ClientSession() as session:
#         async with session.get(poster_url) as resp:
#             with open(local_filename, "wb") as f:
#                 f.write(await resp.read())
#     print(f"[Poster] Saved locally as: {local_filename}")

#     # Upload to blob
#     blob_name = f"{uuid4()}-{local_filename}"
#     print(f"[Blob] Uploading to Azure as: {blob_name}")
#     blob_client = container_client.get_blob_client(blob_name)
#     with open(local_filename, "rb") as data_file:
#         await blob_client.upload_blob(data_file, overwrite=True)
#     thumbnail_url = blob_client.url
#     print(f"[Blob] Uploaded thumbnail URL: {thumbnail_url}")

#     os.remove(local_filename)
#     print(f"[Clean] Removed local poster file: {local_filename}")

#     # Insert movie
#     movie_id = actualTitle.replace(" ", "_")
#     print(f"[Agent] Processing movie: {actualTitle}\n ID: {movie_id}")
#     print(f"[Gremlin] Inserting movie node for: {actualTitle}")
#     insert_movie = f"""
#     g.V().has('movie','id','{movie_id}').fold().coalesce(
#         unfold(),
#         addV('movie')
#         .property('id','{movie_id}')
#         .property('title','{actualTitle}')
#         .property('genre','{genre}')
#         .property('year','{year}')
#         .property('thumbnail','{thumbnail_url}')
#     )
#     """
#     gremlin_client.submitAsync(insert_movie)

#     # Insert directors
#     for director in director_list:
#         dir_id = director.replace(" ", "_")
#         print(f"[Gremlin] Upserting director: {director} {dir_id}")
#         gremlin_client.submitAsync(f"""
#         g.V().has('director','id','{dir_id}').fold().coalesce(
#             unfold(),
#             addV('director')
#               .property('id','{dir_id}')
#               .property('name','{director}')
#               .property('genre','{genre}')
#         )
#         """)
#         print(f"[Gremlin] Creating Directed edge: {director}   {title}")
#         gremlin_client.submitAsync(f"""
#         g.V().has('director','id','{dir_id}')
#           .addE('Directed')
#           .to(g.V().has('movie','id','{movie_id}'))
#         """)

#     # Insert actors
#     for actor in actor_list:
#         act_id = actor.replace(" ", "_")
#         print(f"[Gremlin] Upserting actor: {actor}   {act_id}")
#         gremlin_client.submitAsync(f"""
#         g.V().has('actor','id','{act_id}').fold().coalesce(
#             unfold(),
#             addV('actor')
#               .property('id','{act_id}')
#               .property('name','{actor}')
#               .property('genre','{genre}')
#         )
#         """)
#         print(f"[Gremlin] Creating ActedIn edge: {title}   {actor}")
#         gremlin_client.submitAsync(f"""
#         g.V().has('movie','id','{movie_id}').as('m')
#           .V().has('actor','id','{act_id}')
#           .addE('ActedIn').from('m')
#         """)

#     print(f"[Done] Movie '{actualTitle}' inserted successfully with full metadata.")
#     return f"Movie '{actualTitle}' inserted with metadata and thumbnail!"
