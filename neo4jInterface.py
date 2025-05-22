from neo4j import GraphDatabase, basic_auth
import json
import time
import requests
import os # Import os

# Remove hardcoded credentials
# NEO4J_URI = "neo4j://192.168.1.203"
# NEO4J_USER = "neo4j"
# NEO4J_PASSWORD = "arch3r12!@"


class PersonConnector:
    def __init__(self, uri, user, password): # Keep constructor arguments for flexibility if needed elsewhere

        # Use provided arguments or fallback to environment variables
        db_uri = uri or os.getenv("NEO4J_URI")
        db_user = user or os.getenv("NEO4J_USER")
        db_password = password or os.getenv("NEO4J_PASSWORD")

        if not all([db_uri, db_user, db_password]):
            raise ValueError("Neo4j connection details (URI, USER, PASSWORD) not found in environment variables or arguments.")

        self._driver = GraphDatabase.driver(db_uri, auth=basic_auth(db_user, db_password))

        self._ensure_indexes()
        print("Neo4j Driver Initialized.")

    def close(self):

        if self._driver:
            self._driver.close()
            print("Neo4j Driver Closed.")

    def _ensure_indexes(self):

        with self._driver.session(database="neo4j") as session:
            session.execute_write(self._create_person_name_index)

    def get_shortest_path(self, person1, person2):
        query = """
        MATCH (p1:Person {name: $person1}), (p2:Person {name: $person2}),
            path = shortestPath((p1)-[*..10]-(p2))
        RETURN nodes(path) AS path_nodes, relationships(path) AS path_rels
        """
        with self._driver.session(database="neo4j") as session:
            result = session.run(query, person1=person1, person2=person2)
            record = result.single()
            if record:
                return self.parseShortestPath(record)
            else:
                return None

    def parseShortestPath(self, record):
        names_list = [dict(node).get("name") for node in record["path_nodes"]]
        relationships = []
        for rel in record["path_rels"]:
            asset = dict(rel).get("asset")
            relationship = json.loads(asset)
            keys_to_keep = ["id","thumbUrl","caption","people","artist","landingUrl","dateCreated"]
            relationship = {key: relationship[key] for key in keys_to_keep if key in relationship}

            relationship["relid"] = rel.id
            relationships.append(relationship)
        return {"names": names_list, "relationships": relationships}

    @staticmethod
    def _create_person_name_index(tx):

        print("Ensuring index on :Person(name)...")

        query = "CREATE INDEX person_name_index IF NOT EXISTS FOR (n:Person) ON (n.name)"
        tx.run(query)
        print("Index check complete.")

    def export_all_nodes(self, output_file="all_nodes.json"):

        print("Exporting nodes...")
        query = "MATCH (n) RETURN n"
        with self._driver.session(database="neo4j") as session:
            result = session.run(query)
            nodes = []
            for record in result:
                node = record["n"]
                node_data = dict(node)


                nodes.append(node_data)

            with open(output_file, "w") as f:
                json.dump(nodes, f, indent=4)

            print(f"Exported {len(nodes)} nodes to {output_file}")

    def update_relationship_rating(self, relid, rating_value):
        """
        Update the rating property of a relationship in Neo4j.
        If the property doesn't exist yet, it will be initialized to 0 before adding the rating.

        Args:
            relid (str): The relationship ID to update
            rating_value (int): The rating value (1 for like, -1 for dislike)

        Returns:
            int or None: The new rating value after update, or None if the relationship wasn't found
        """

        query = """
        MATCH ()-[r]->()
        WHERE id(r) = $relid
        SET r.rating = COALESCE(r.rating, 0) + $rating_value
        RETURN r.rating as new_rating, id(r) as rel_id
        """

        try:
            print(f"Attempting to update relationship with ID: {relid}, rating: {rating_value}")
            with self._driver.session(database="neo4j") as session:

                try:
                    int_id = int(relid)
                    result = session.run(query, relid=int_id, rating_value=rating_value)


                    records = list(result)
                    if records:
                        print(f"Successfully updated relationship {relid}, new rating: {records[0]['new_rating']}")
                        return records[0]["new_rating"]
                    else:
                        print(f"No relationship found with ID: {relid}")
                        return None
                except ValueError:
                    print(f"Relationship ID '{relid}' is not a valid integer")
                    return None
        except Exception as e:
            print(f"Error updating relationship rating: {e}")
            return None

    def add_connection(self, people, is_new_person, asset):
        """
        Add a new connection between people in the database.
        Creates new person nodes if necessary.
        
        Args:
            people (list): List of person names
            is_new_person (list): List of booleans indicating if the person is new
            asset (dict): The asset data (photo/connection info)
            
        Returns:
            bool: True if successful, False otherwise
        """
        # Ensure we have at least 2 people
        if len(people) < 2:
            print("Need at least 2 people to create a connection")
            return False
            
        try:
            # Convert asset to JSON string
            asset_json = json.dumps(asset)
            
            with self._driver.session(database="neo4j") as session:
                # Create a transaction function
                def create_connection(tx):
                    # First, create or get all person nodes
                    person_refs = []
                    
                    for i, person in enumerate(people):
                        # Clean the name if needed (you might want to add a name cleaning function)
                        cleaned_name = person.strip()
                        
                        if is_new_person[i]:
                            # For new people, create a new node
                            query = """
                            CREATE (p:Person {name: $name, name_cleaned: $name_cleaned})
                            RETURN p
                            """
                            result = tx.run(query, name=cleaned_name, name_cleaned=cleaned_name.split(" - ")[0])
                            person_node = result.single()[0]
                            person_refs.append(person_node)
                        else:
                            # For existing people, merge to avoid duplicates
                            query = """
                            MATCH (p:Person {name: $name})
                            RETURN p
                            """
                            result = tx.run(query, name=cleaned_name)
                            record = result.single()
                            if record:
                                person_refs.append(record[0])
                            else:
                                # Create if doesn't exist (shouldn't happen if properly validated)
                                create_query = """
                                CREATE (p:Person {name: $name, name_cleaned: $name_cleaned})
                                RETURN p
                                """
                                create_result = tx.run(create_query, name=cleaned_name, name_cleaned=cleaned_name.split(" - ")[0])
                                new_person = create_result.single()[0]
                                person_refs.append(new_person)
                    
                    # Now create relationships between each person and all others
                    for i in range(len(person_refs)):
                        for j in range(i+1, len(person_refs)):
                            # First check if relationship already exists between these people
                            check_query = """
                            MATCH (p1)-[r:IN_PICTURE_WITH]-(p2)
                            WHERE id(p1) = $id1 AND id(p2) = $id2
                            RETURN r
                            """
                            existing_rel = tx.run(
                                check_query,
                                id1=person_refs[i].id,
                                id2=person_refs[j].id
                            ).single()
                            
                            # Only create relationship if it doesn't exist
                            if not existing_rel:
                                query = """
                                MATCH (p1) WHERE id(p1) = $id1
                                MATCH (p2) WHERE id(p2) = $id2
                                CREATE (p1)-[r:IN_PICTURE_WITH {asset: $asset, rating: 0}]->(p2)
                                RETURN r
                                """
                                tx.run(
                                    query, 
                                    id1=person_refs[i].id, 
                                    id2=person_refs[j].id, 
                                    asset=asset_json
                                )
                            else:
                                print(f"Relationship already exists between {i} and {j}, skipping creation.")
                    
                    return True
                
                # Execute the transaction
                result = session.execute_write(create_connection)
                return result
                
        except Exception as e:
            print(f"Error adding connection: {e}")
            return False

    def delete_relationship(self, relid):
        """
        Delete a relationship by ID from Neo4j.
        
        Args:
            relid (str): The relationship ID to delete
            
        Returns:
            bool: True if successful, False otherwise
        """
        query = """
        MATCH ()-[r]->()
        WHERE id(r) = $relid
        DELETE r
        RETURN count(r) as deleted_count
        """
        
        try:
            print(f"Attempting to delete relationship with ID: {relid}")
            with self._driver.session(database="neo4j") as session:
                try:
                    int_id = int(relid)
                    result = session.run(query, relid=int_id)
                    
                    summary = result.consume()
                    if summary.counters.relationships_deleted > 0:
                        print(f"Successfully deleted relationship {relid}")
                        return True
                    else:
                        print(f"No relationship found with ID: {relid}")
                        return False
                except ValueError:
                    print(f"Relationship ID '{relid}' is not a valid integer")
                    return False
        except Exception as e:
            print(f"Error deleting relationship: {e}")
            return False




