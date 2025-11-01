import os, httpx, tempfile, asyncio, traceback
from uuid import uuid4
from mcp.server.fastmcp import FastMCP
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient
from gremlin_python.driver.client import Client
from gremlin_python.driver.serializer import GraphSONSerializersV2d0

load_dotenv()

# mcp = FastMCP("neuraflix-mcp")
mcp = FastMCP(
    name="NeuraFlixMCP",
    host="0.0.0.0",  # only used for SSE transport (localhost)
    port=8000,  # only used for SSE transport (set this to any port)
)

OMDB_API_KEY = os.getenv("OMDB_API_KEY")
OMDB_BASE_URL = "http://www.omdbapi.com"

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
    def _upload():
        conn = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        container = os.getenv("AZURE_STORAGE_CONTAINER_NAME")
        bs = BlobServiceClient.from_connection_string(conn)
        cc = bs.get_container_client(container)
        blob_name = f"{uuid4()}-{os.path.basename(local_path)}"
        blob = cc.get_blob_client(blob_name)
        with open(local_path, "rb") as f:
            blob.upload_blob(f, overwrite=True)
        print(f"[Blob] Uploaded {local_path} as {blob_name}")
        return blob.url
    return await run_blocking(_upload)

async def gremlin_insert(movie_id, title, year, genre, thumb, directors, actors):
    def _work():
        endpoint = os.getenv("GREMLIN_ENDPOINT")
        db = os.getenv("GREMLIN_DB_NAME")
        graph = os.getenv("GREMLIN_COLLECTION")
        key = os.getenv("GREMLIN_PK")

        client = Client(
            endpoint, 'g',
            username=f"/dbs/{db}/colls/{graph}",
            password=key,
            message_serializer=GraphSONSerializersV2d0()
        )
        try:
            # movie
            print(f"[Gremlin] Upserting movie {movie_id}")
            client.submit(f"""
              g.V().has('movie','id','{movie_id}').fold().coalesce(
                unfold(),
                addV('movie')
                  .property('id','{movie_id}')
                  .property('title','{title}')
                  .property('genre','{genre}')
                  .property('year','{year}')
                  .property('thumbnail','{thumb}')
              )""").all().result()

            # directors
            for d in directors:
                d_id = d.replace(" ", "_")
                print(f"[Gremlin] Upserting director {d_id}")
                client.submit(f"""
                  g.V().has('director','id','{d_id}').fold().coalesce(
                    unfold(),
                    addV('director')
                      .property('id','{d_id}')
                      .property('name','{d}')
                      .property('genre','{genre}')
                  )""").all().result()

                print(f"[Gremlin] Linking director {d_id} -> movie {movie_id}")
                client.submit(f"""
                  g.V().has('director','id','{d_id}')
                   .addE('Directed')
                   .to(g.V().has('movie','id','{movie_id}'))
                """).all().result()

            # actors
            for a in actors:
                a_id = a.replace(" ", "_")
                print(f"[Gremlin] Upserting actor {a_id}")
                client.submit(f"""
                  g.V().has('actor','id','{a_id}').fold().coalesce(
                    unfold(),
                    addV('actor')
                      .property('id','{a_id}')
                      .property('name','{a}')
                      .property('genre','{genre}')
                  )""").all().result()

                print(f"[Gremlin] Linking movie {movie_id} -> actor {a_id}")
                client.submit(f"""
                  g.V().has('movie','id','{movie_id}').as('m')
                   .V().has('actor','id','{a_id}')
                   .addE('ActedIn').from('m')
                """).all().result()

        except Exception:
            print("[Gremlin] Error during insert:")
            traceback.print_exc()
        finally:
            client.close()

    await run_blocking(_work)


from langchain_groq import ChatGroq
from langchain_core.prompts import ChatPromptTemplate

@mcp.tool()
async def insert_movies_from_prompt(user_prompt: str) -> str:
    """
    Accepts a user prompt (e.g., 'Insert Harry Potter movie series'),
    uses the LLM to extract or generate a list of real movie titles,
    and inserts them into the NeuraFlix knowledge graph.
    """
    # Ensure GROQ_API_KEY is loaded
    if not os.getenv("GROQ_API_KEY"):
        load_dotenv()
        os.environ["GROQ_API_KEY"] = os.getenv("GROQ_API_KEY")

    try:
        # Prompt to extract movie titles
        prompt = ChatPromptTemplate.from_messages([
            ("system", "You're an assistant that extracts real, valid movie titles from a user prompt. "
                       "Return a numbered list of actual movie titles only, without extra commentary."),
            ("user", "{prompt}")
        ])
        messages = prompt.format_messages(prompt=user_prompt)

        llm = ChatGroq(model="qwen/qwen3-32b")
        response = await llm.ainvoke(messages)
        content = response.content.strip()

        # Parse the LLM's response into movie title list
        lines = content.splitlines()
        titles = []
        for line in lines:
            # Extract titles from numbered list like "1. Harry Potter and the Sorcerer's Stone"
            if "." in line:
                title = line.split(".", 1)[1].strip().strip('"')
                if title:
                    titles.append(title)
            else:
                # fallback for unnumbered plain lines
                cleaned = line.strip().strip('"')
                if cleaned:
                    titles.append(cleaned)

        if not titles:
            return "No valid movie titles were extracted from the prompt."

        result_msgs = []
        for title in titles:
            result = await insert_movie_with_details(title)
            result_msgs.append(f"✔️ {title}: {result}")

        return "\n".join(result_msgs)

    except Exception as e:
        return f"[Error] Could not process movies from prompt: {e}"

@mcp.tool()
async def insert_movie_with_details(title: str) -> str:
    """
    Insert a movie and its metadata into the NeuraFlix knowledge graph.

    This tool fetches movie details from the OMDb API using the given title,
    downloads and uploads the poster image to Azure Blob Storage, and inserts
    the movie, its directors, and actors into the Cosmos DB Gremlin graph database.
    """
    data = await fetch_omdb(title)
    if not data or data.get("Response")=="False":
        return f"Movie not found: {title}"

    at = data["Title"]
    year = data.get("Year","Unknown")
    genre = data.get("Genre","Unknown")
    poster = data.get("Poster")
    directors = [d.strip() for d in data.get("Director","").split(",") if d.strip() and d.strip() != "N/A"]
    actors = [a.strip() for a in data.get("Actors","").split(",") if a.strip() and a.strip() != "N/A"]

    # poster download
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".jpg")
    try:
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.get(poster)
            r.raise_for_status()
            tmp.write(r.content)
    except Exception as e:
        print("[Poster] Download failed:", e)
        tmp.close()
        return "Metadata fetched; poster download failed."
    tmp.close()

    # upload
    thumb_url = await upload_blob(tmp.name)
    os.remove(tmp.name)
    if not thumb_url:
        return "Metadata fetched; poster upload failed."

    movie_id = at.replace(" ","_")
    await gremlin_insert(movie_id, at, year, genre, thumb_url, directors, actors)

    return f"Inserted: {at}"

@mcp.resource("test://{msg}")
def test(msg: str) -> str:
    return f"ok: {msg}"

# To run the MCP server remotely
if __name__ == "__main__":
    transport = "sse"
    if transport == "stdio":
        print("Running server with stdio transport")
        mcp.run(transport="stdio")
    elif transport == "sse":
        print("Running server with SSE transport")
        mcp.run(transport="sse")
    else:
        raise ValueError(f"Unknown transport: {transport}")