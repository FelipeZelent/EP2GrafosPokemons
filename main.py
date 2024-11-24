import csv
from neo4j import GraphDatabase
import ast

class PokemonGraph:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    # Insere um Pokémon e suas relações no Neo4j
    def insert_pokemon(self, tx, pokemon):
        tx.run("""
            MERGE (p:Pokemon {id: $pokemon_id})
            SET p.name = $pokemon_name,
                p.height = $pokemon_height,
                p.weight = $pokemon_weight,
                p.url = $pokemon_url
            WITH p
            UNWIND $pokemon_types AS type
            MERGE (t:Type {name: type})
            MERGE (p)-[:HAS_TYPE]->(t)
            WITH p
            UNWIND $pokemon_abilities AS ability
            MERGE (a:Ability {name: ability.name, url: ability.url})
            MERGE (p)-[:HAS_ABILITY]->(a)
            WITH p
            UNWIND $pokemon_evolutions AS evolution
            MERGE (e:Pokemon {id: evolution.id, name: evolution.name, url: evolution.url})
            MERGE (p)-[:EVOLVES_TO]->(e)
        """,
        pokemon_id=pokemon['id'],
        pokemon_name=pokemon['name'],
        pokemon_height=pokemon['height_cm'],
        pokemon_weight=pokemon['weight_kg'],
        pokemon_url=pokemon['page_url'],
        pokemon_types=pokemon['type_list'],
        pokemon_abilities=pokemon['abilities'],
        pokemon_evolutions=pokemon['evolutions'])

    # Insere todos os Pokémons do CSV no banco de dados
    def insert_pokemons(self, pokemons):
        with self.driver.session() as session:
            for pokemon in pokemons:
                print(f"Inserindo Pokémon: {pokemon['name']} (ID: {pokemon['id']})")
                session.execute_write(self.insert_pokemon, pokemon)

def processar_lista(campo):
    return [item.strip() for item in campo.split(',') if item.strip()]

def processar_lista_dicionarios(campo):
    try:
        return ast.literal_eval(campo)
    except (SyntaxError, ValueError):
        return []

def ler_csv_para_lista(nome_arquivo):
    pokemons = []
    with open(nome_arquivo, 'r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        print("Cabeçalhos encontrados no CSV:", reader.fieldnames)
        for row in reader:
            try:
                row['type_list'] = processar_lista(row['type_list'])
                row['abilities'] = processar_lista_dicionarios(row['abilities'])
                row['evolutions'] = processar_lista_dicionarios(row['evolutions'])
                pokemons.append(row)
            except Exception as e:
                print(f"Erro ao processar linha: {row}. Detalhes do erro: {e}")
    return pokemons

# Configuração da conexão
uri = "bolt+s://974b89f1.databases.neo4j.io:7687"
username = "neo4j"
password = "YdF041GNIPORIxDDS9z12_mUQwXC_DBxQ7jcqx1banY"

try:
    graph = PokemonGraph(uri, username, password)
    print("Conexão com o Neo4j estabelecida com sucesso.")

    # Carrega os dados do CSV e insere no banco
    pokemons = ler_csv_para_lista('pokemons.csv')
    graph.insert_pokemons(pokemons)

    print("Inserção concluída com sucesso.")
    graph.close()
except Exception as e:
    print("Erro:", e)
