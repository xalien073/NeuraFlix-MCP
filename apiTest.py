import aiohttp
import asyncio

OMDB_API_KEY = "610e6a03"

async def fetch_movie_data(title):
    api_url = f"http://www.omdbapi.com/?t={title}&apikey={OMDB_API_KEY}"
    
    async with aiohttp.ClientSession() as session:
        async with session.get(api_url) as response:
            data = await response.json()
            print('[OMDb] Raw data:', data["Response"])

    if data.get("Response") == "False":
        print(f"[OMDb] Movie not found: {title}")
        return {"error": f"Movie not found in OMDb: {title}"}

    for k, v in data.items():
        print(f"{k}: {v}")

        
    return {
        "title": data.get("Title", "Unknown"),
        "year": data.get("Year", "Unknown"),
        "genre": data.get("Genre", "Unknown"),
        "directors": [d.strip() for d in data.get("Director", "").split(",")],
        "actors": [a.strip() for a in data.get("Actors", "").split(",")],
        "poster_url": data.get("Poster", "")
    }

# To test:
if __name__ == "__main__":
        movie_title = "Ice Age"
    result = asyncio.run(fetch_movie_data(movie_title))
    print("Parsed movie data:", result)

"""API Response
Title: Toy Story                                                                                                                                                                                                
Year: 1995                                                                                                                                                                                                      
Rated: G                                                                                                                                                                                                        
Released: 22 Nov 1995                                                                                                                                                                                           
Runtime: 81 min                                                                                                                                                                                                 
Genre: Animation, Adventure, Comedy                                                                                                                                                                             
Director: John Lasseter                                                                                                                                                                                         
Writer: John Lasseter, Pete Docter, Andrew Stanton                                                                                                                                                              
Actors: Tom Hanks, Tim Allen, Don Rickles                                                                                                                                                                       
Plot: A cowboy doll is profoundly jealous when a new spaceman action figure supplants him as the top toy in a boy's bedroom. When circumstances separate them from their owner, the duo have to put aside their differences to return to him.                                                                                                                                                                                   
Language: English                                                                                                                                                                                               
Country: United States                                                                                                                                                                                          
Awards: Nominated for 3 Oscars. 29 wins & 24 nominations total                                                                                                                                                  
Poster: https://m.media-amazon.com/images/M/MV5BZTA3OWVjOWItNjE1NS00NzZiLWE1MjgtZDZhMWI1ZTlkNzYwXkEyXkFqcGc@._V1_SX300.jpg                                                                                      
Ratings: [{'Source': 'Internet Movie Database', 'Value': '8.3/10'}, {'Source': 'Rotten Tomatoes', 'Value': '100%'}, {'Source': 'Metacritic', 'Value': '96/100'}]                                                
Metascore: 96                                                                                                                                                                                                   
imdbRating: 8.3                                                                                                                                                                                                 
imdbVotes: 1,126,500                                                                                                                                                                                            
imdbID: tt0114709                                                                                                                                                                                               
Type: movie                                                                                                                                                                                                     
DVD: N/A                                                                                                                                                                                                        
BoxOffice: $223,225,679                                                                                                                                                                                         
Production: N/A                                                                                                                                                                                                 
Website: N/A                                                                                                                                                                                                    
Response: True                                                                                                                                                                                                  
"""