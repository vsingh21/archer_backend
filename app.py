from flask import Flask, request
from neo4jInterface import *
from flask_cors import CORS
import json
import requests  # Add this import for the new endpoint
from rapidfuzz import process, fuzz
from rapidfuzz.process import extract, extractOne, cdist
from apscheduler.schedulers.background import BackgroundScheduler
from collections import defaultdict
from supabase import create_client, Client
import os
from datetime import datetime
import uuid

app = Flask(__name__)
CORS(app)

# Neo4j configuration from environment variables
NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")
connector = PersonConnector(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)

# Supabase configuration
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
SUPABASE_BUCKET = os.getenv("SUPABASE_BUCKET", "connection-images")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

all_nodes = None

def load_all_nodes():
    global all_nodes, name_cleaned_list, name_display_map
    print("Loading all nodes...")
    with open('all_nodes.json', 'r') as file:
        all_nodes = json.load(file)

        name_cleaned_list = [entry['name_cleaned'] for entry in all_nodes]

        # Allow multiple names for the same cleaned name
        name_display_map = defaultdict(list)
        for entry in all_nodes:
            name_display_map[entry['name_cleaned']].append(entry['name'])
export_scheduler = BackgroundScheduler(daemon=True)
export_scheduler.add_job(connector.export_all_nodes, 'interval', minutes=1)  # Run every hour
export_scheduler1 = BackgroundScheduler(daemon=True)
export_scheduler1.add_job(load_all_nodes, 'interval', minutes=1)  # Run every hour
export_scheduler.start()
export_scheduler1.start()


name_cleaned_list = []
name_display_map = {}

init_flag = False

@app.before_request
def startup():
    global init_flag
    if not init_flag:
        connector.export_all_nodes()
        load_all_nodes()
        init_flag = True

    

@app.route('/api/getPath', methods=['GET'])
def getInfo():
    person1 = request.args.get('person1')
    person2 = request.args.get('person2')
    if not person1 or not person2:
        return "Missing person1 or person2 parameter", 400
    if person1 == person2:
        return f"person1 and person2 are the same: {person1}", 400
    
    path = connector.get_shortest_path(person1, person2)
    if path:
        # Track this search in Supabase and get the count
        count = track_search(person1, person2)
        # Add the count to the path dictionary
        path["timesVisited"] = count
        return path
    else:
        return f"No path found between {person1} and {person2}", 404

def track_search(person1, person2):
    """Record a search between two people in Supabase and return the count"""
    try:
        # Sort names alphabetically for consistent entry
        names = sorted([person1, person2])
        
        # Check if this pair already exists
        response = supabase.table("connection_searches").select("*").eq("person1", names[0]).eq("person2", names[1]).execute()
        
        if response.data and len(response.data) > 0:
            # Update existing record
            search_record = response.data[0]
            count = search_record.get("count", 0) + 1
            
            supabase.table("connection_searches").update({
                "count": count, 
                "last_searched": datetime.now().isoformat()
            }).eq("id", search_record["id"]).execute()
            return count
        else:
            # Create new record
            supabase.table("connection_searches").insert({
                "person1": names[0],
                "person2": names[1],
                "count": 1,
                "first_searched": datetime.now().isoformat(),
                "last_searched": datetime.now().isoformat()
            }).execute()
            return 1
    except Exception as e:
        print(f"Error tracking search in Supabase: {e}")
        return 0

@app.route('/api/autocomplete', methods=['GET'])
def getAutocomplete():
    person = request.args.get('person')
    if not person:
        return "Missing person1 or person2 parameter", 400
    results = fuzzy_search_people(person.lower())
    return results

def word_match_score(query, name):
    """Calculate a score for how well each word in the query matches words in the name"""
    query_words = query.lower().split()
    name_words = name.lower().split()
    
    # If there are no words in the query or name, return 0
    if not query_words or not name_words:
        return 0
    
    total_score = 0
    matched_words = 0
    
    for q_word in query_words:
        best_word_score = 0
        for n_word in name_words:
            # Exact word match
            if q_word == n_word:
                word_score = 10
            # Word starts with query word
            elif n_word.startswith(q_word):
                word_score = 8
            # Query word starts with word
            elif q_word.startswith(n_word):
                word_score = 6
            # Word contains query word
            elif q_word in n_word:
                word_score = 4
            # Some fuzzy match
            else:
                # Use token sort ratio for fuzzy word matching
                word_score = fuzz.ratio(q_word, n_word) / 20  # Scale down to 0-5
            
            best_word_score = max(best_word_score, word_score)
        
        if best_word_score > 0:
            matched_words += 1
            total_score += best_word_score
    
    # Multiply by the ratio of query words that matched
    match_ratio = matched_words / len(query_words)
    final_score = total_score * match_ratio
    
    return final_score

def fuzzy_search_people(query, limit=10, cutoff=40):
    # Skip processing for very short queries (1-2 characters)
    if len(query.strip()) < 3:
        # For very short queries, only return exact prefix matches
        prefix_matches = []
        query_lower = query.lower()
        
        for name in name_cleaned_list:
            name_lower = name.lower()
            # Only include exact matches or names that start with the query
            if name_lower == query_lower or name_lower.startswith(query_lower):
                prefix_matches.append((name, 100 if name_lower == query_lower else 95, 0))
        
        # Sort by score and then by name length
        prefix_matches.sort(key=lambda x: (-x[1], len(x[0])))
        
        seen = set()
        final = []
        
        for name_cleaned, _, _ in prefix_matches:
            display_names = name_display_map[name_cleaned]
            display_names.sort(key=len)
            
            for display_name in display_names:
                if display_name not in seen:
                    seen.add(display_name)
                    final.append(display_name)
                    
                    if len(final) >= limit:
                        return final
        
        return final

    # For normal length queries, use the full matching logic
    # Step 1: Prioritize prefix matches first
    prefix_matches = []
    contains_matches = []
    word_matches = []
    other_matches = []
    
    # Parse query into words
    query_lower = query.lower()
    query_words = [w for w in query_lower.split() if len(w) > 1]
    
    for name in name_cleaned_list:
        name_lower = name.lower()
        word_score = word_match_score(query_lower, name_lower)
        
        # Exact match gets highest priority
        if name_lower == query_lower:
            prefix_matches.append((name, 100, 0))
        # Names that start with the query get high priority
        elif name_lower.startswith(query_lower):
            prefix_matches.append((name, 95, 0))
        # Names that contain all query words as distinct parts get medium priority
        elif query_words and all(q in name_lower.split() for q in query_words):
            contains_matches.append((name, 90, 0))
        # Names that contain the query as a substring get lower priority
        elif query_lower in name_lower:
            contains_matches.append((name, 85, 0))
        # Names that have good word-by-word matches
        elif word_score > 5:
            word_matches.append((name, 80 + min(word_score, 10), 0))
    
    # Sort prefix and contains matches by name length (shorter names first)
    prefix_matches.sort(key=lambda x: len(x[0]))
    contains_matches.sort(key=lambda x: len(x[0]))
    word_matches.sort(key=lambda x: x[1], reverse=True)  # Sort by score
    
    # Only use fuzzy search if we don't have enough direct matches
    combined_matches = prefix_matches + contains_matches + word_matches
    if len(combined_matches) < limit:
        # Step 2: Use fuzzy search for remaining matches
        fuzzy_results = process.extract(
            query,
            name_cleaned_list,
            scorer=fuzz.WRatio,
            limit=limit * 2,
            score_cutoff=cutoff
        )
        
        # Filter out any results already in our direct matches
        existing_names = {name for name, _, _ in combined_matches}
        
        # Apply more filtering to fuzzy matches to ensure they're relevant
        filtered_fuzzy = []
        for name, score, idx in fuzzy_results:
            if name in existing_names:
                continue
                
            name_lower = name.lower()
            
            # Filter out fuzzy matches that don't have any relationship to the query
            # At least one query word should be partially in the name
            if not any(q in name_lower for q in query_words if len(q) > 1):
                # If no direct substring match, require a higher fuzzy score
                if score < 75:  # Increase the threshold for fuzzy matches
                    continue
            
            filtered_fuzzy.append((name, score, idx))
        
        # Add fuzzy matches to our results
        other_matches = filtered_fuzzy
    
    # Combine all matches with prefix matches first, then contains matches, then fuzzy matches
    all_results = prefix_matches + contains_matches + word_matches + other_matches
    
    # Step 3: Track unique display names to avoid duplicates
    seen = set()
    final = []
    
    # Step 4: Map cleaned names to their display names
    for name_cleaned, score, _ in all_results:
        # Get all display variants for this cleaned name
        display_names = name_display_map[name_cleaned]
        
        # Sort display names to prioritize shorter, more common versions
        display_names.sort(key=len)
        
        # Add each unique display name to results
        for display_name in display_names:
            if display_name not in seen:
                seen.add(display_name)
                final.append(display_name)
                
                # Once we reach our limit, return results
                if len(final) >= limit:
                    return final
    
    return final

@app.route('/api/updateRating', methods=['GET'])
def updateRating():
    relid = request.args.get('relid')
    is_like = request.args.get('isLike')
    
    if not relid or is_like is None:
        return "Missing relid or isLike parameter", 400
    
    print(f"Received rating update request for relationship {relid}, isLike={is_like}")
    
    # Convert string parameter to boolean, then to integer value
    is_like_bool = is_like.lower() in ('true', '1', 't', 'y', 'yes')
    rating_value = 1 if is_like_bool else -1
    
    try:
        # Update the rating in Neo4j
        new_rating = connector.update_relationship_rating(relid, rating_value)
        
        if new_rating is not None:
            return f"Updated relationship {relid} with rating {rating_value}, new total: {new_rating}", 200
        else:
            print(f"Relationship {relid} not found in database")
            return f"Relationship {relid} not found", 404
    except Exception as e:
        print(f"Error updating rating: {e}")
        return f"Error updating rating: {str(e)}", 500

@app.route('/api/getEmbed', methods=['GET'])
def getEmbed():
    landing_url = request.args.get('landingUrl')
    if not landing_url:
        return "Missing landingUrl parameter", 400
    
    try:
        # Only process URLs that match the expected pattern
        if "/detail/" in landing_url:
            # Ensure landingUrl starts with a / for proper joining
            if not landing_url.startswith('/'):
                landing_url = '/' + landing_url
                
            full_url = "https://www.gettyimages.com" + landing_url
            response = requests.get(f"https://iframely.com/api/try?url={full_url}")
            if response.status_code == 200:
                embed_data = json.loads(response.text)
                embed_html = embed_data.get("code", "")
                print(f"Returning embed HTML: {embed_html[:100]}...") 
                return {"embedHTML": embed_html}
            else:
                print(f"Error fetching embed: HTTP {response.status_code}")
                return f"Error fetching embed: {response.status_code}", 500
        else:
            return {"embedHTML": ""}, 200
    except Exception as e:
        print(f"Error getting embed HTML: {e}")
        return f"Error getting embed HTML: {str(e)}", 500

@app.route('/api/addUserConnection', methods=['POST'])
def add_user_connection():
    try:
        # Get form data
        name = request.form.get('name')
        email = request.form.get('email')
        description = request.form.get('description')
        date = request.form.get('date')
        image_source = request.form.get('image_source', 'upload')
        is_owner = request.form.get('is_owner', 'false').lower() == 'true'
        
        # Get ownership information
        owner_name = ""
        landing_url = ""
        public_acknowledgment = False
        
        if is_owner:
            public_acknowledgment = request.form.get('public_acknowledgment', 'false').lower() == 'true'
        else:
            owner_name = request.form.get('owner_name', '')
            landing_url = request.form.get('landing_url', '')
        
        # Get people data
        people = []
        is_new = []
        
        # Extract people data from form
        for key in request.form:
            if key.startswith('people['):
                index = key[7:-1]  # Extract index number
                person = request.form.get(key)
                people.append(person)
            elif key.startswith('isNew['):
                index = key[6:-1]  # Extract index number
                new_status = request.form.get(key).lower() == 'true'
                is_new.append(new_status)
        
        # Process image - either from file upload or URL
        image_url = None
        
        if image_source == 'upload' and 'photo' in request.files:
            photo = request.files.get('photo')
            
            # Create a unique filename
            filename = f"{uuid.uuid4()}.{photo.filename.split('.')[-1]}"
            
            # Save to temporary file first
            temp_filepath = os.path.join('/tmp', filename)
            photo.save(temp_filepath)
            
            try:
                # Upload to Supabase Storage
                with open(temp_filepath, 'rb') as f:
                    file_data = f.read()
                
                # Upload the file to Supabase bucket
                upload_response = supabase.storage.from_(SUPABASE_BUCKET).upload(
                    path=filename,
                    file=file_data,
                    file_options={"content-type": photo.content_type}
                )
                
                # Get the public URL
                image_url = supabase.storage.from_(SUPABASE_BUCKET).get_public_url(filename)
                
                # Clean up temporary file
                os.remove(temp_filepath)
                
            except Exception as e:
                print(f"Error uploading to Supabase: {e}")
                # Fallback to local storage if Supabase upload fails
                filepath = os.path.join('uploads', filename)
                os.makedirs('uploads', exist_ok=True)
                photo.save(filepath)
                image_url = filepath
        
        elif image_source == 'url' and 'photo_url' in request.form:
            # Use the provided URL directly
            image_url = request.form.get('photo_url')
        
        # Create contribution entry in Supabase
        contribution_data = {
            "name": name,
            "email": email,
            "description": description,
            "date": date,
            "photo_path": image_url,
            "people": json.dumps(people),
            "is_new_person": json.dumps(is_new),
            "is_owner": is_owner,
            "owner_name": owner_name,
            "landing_url": landing_url,
            "public_acknowledgment": public_acknowledgment,
            "status": "pending",  # Initial status is pending
            "created_at": datetime.now().isoformat()
        }
        
        result = supabase.table("contributions").insert(contribution_data).execute()
        
        return {"message": "Contribution submitted successfully"}, 200
    
    except Exception as e:
        print(f"Error submitting contribution: {e}")
        return {"message": f"Error: {str(e)}"}, 500

@app.route('/api/getContributions', methods=['GET'])
def get_contributions():
    try:
        # Get JWT token from request
        auth_header = request.headers.get('Authorization')
        if not auth_header or not auth_header.startswith('Bearer '):
            return {"message": "Unauthorized - Missing or invalid token"}, 401
        
        token = auth_header.split(' ')[1]
        
        # Verify the token (simple check - in production, you'd want to verify with Supabase)
        try:
            # Get contributions from Supabase
            result = supabase.table("contributions").select("*").execute()
            contributions = result.data
            
            return {"contributions": contributions}, 200
        except Exception as e:
            return {"message": "Unauthorized - Invalid token"}, 401
    
    except Exception as e:
        print(f"Error fetching contributions: {e}")
        return {"message": f"Error: {str(e)}"}, 500

@app.route('/api/approveContribution', methods=['POST'])
def approve_contribution():
    try:
        # Get JWT token from request
        auth_header = request.headers.get('Authorization')
        if not auth_header or not auth_header.startswith('Bearer '):
            return {"message": "Unauthorized - Missing or invalid token"}, 401
        
        token = auth_header.split(' ')[1]
        
        # Get contribution ID and approval status
        data = request.json
        contribution_id = data.get('contributionId')
        approve = data.get('approve', False)
        
        if not contribution_id:
            return {"message": "Missing contribution ID"}, 400
        
        # Get the contribution data
        result = supabase.table("contributions").select("*").eq("id", contribution_id).execute()
        
        if not result.data:
            return {"message": "Contribution not found"}, 404
        
        contribution = result.data[0]
        
        # Update the status in Supabase
        new_status = "approved" if approve else "rejected"
        supabase.table("contributions").update({"status": new_status}).eq("id", contribution_id).execute()
        
        # If approved, update Neo4j
        if approve:
            # Parse the people data
            people = json.loads(contribution.get("people", "[]"))
            is_new_person = json.loads(contribution.get("is_new_person", "[]"))
            description = contribution.get("description", "")
            date = contribution.get("date", "")
            photo_path = contribution.get("photo_path", "")
            is_owner = contribution.get("is_owner", False)
            owner_name = contribution.get("owner_name", "")
            landing_url = contribution.get("landing_url", "")
            
            # Get the photographer/artist name
            artist_name = contribution.get("name", "Unknown")
            if not is_owner and owner_name:
                artist_name = owner_name
            
            # Create an asset object
            asset = {
                "id": str(uuid.uuid4()),
                "caption": description,
                "dateCreated": date,
                "people": people,
                "thumbUrl": photo_path,  # Using the Supabase URL from the contribution
                "artist": artist_name,
                "landingUrl": landing_url if landing_url else photo_path,  # Use landing URL if provided, otherwise use photo path
                "photoOwner": owner_name if not is_owner else artist_name,
                "isContributed": True
            }
            
            # Update Neo4j with the new connection
            result = connector.add_connection(people, is_new_person, asset)
            
            if not result:
                return {"message": "Failed to update Neo4j database"}, 500
        
        return {"message": f"Contribution {new_status} successfully"}, 200
    
    except Exception as e:
        print(f"Error processing contribution: {e}")
        return {"message": f"Error: {str(e)}"}, 500

def shutdown(exception=None):
    connector.close()