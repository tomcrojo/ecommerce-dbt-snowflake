#!/usr/bin/env python3
"""Generate large seed CSV files for the e-commerce dbt project.

Generates:
  - seeds/raw_customers.csv  (10,000 rows)
  - seeds/raw_products.csv   (200 rows)
  - seeds/raw_orders.csv     (100,000 rows)
  - seeds/raw_order_items.csv (250,000 rows)

All data uses Spanish names, cities, and EUR prices.
Referential integrity is enforced: every order references an existing customer,
every order_item references an existing order + product.

Usage:
    python scripts/generate_large_dataset.py
"""

import csv
import random
from datetime import date, timedelta
from pathlib import Path

from faker import Faker

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

SEED_DIR = Path(__file__).resolve().parent.parent / "seeds"
SEED_DIR.mkdir(parents=True, exist_ok=True)

NUM_CUSTOMERS = 10_000
NUM_PRODUCTS = 200
NUM_ORDERS = 100_000
AVG_ITEMS_PER_ORDER = 2.5  # target average → ~250k line items

DATE_START = date(2023, 1, 1)
DATE_END = date(2026, 3, 21)

fake = Faker("es_ES")
Faker.seed(42)
random.seed(42)

SPAIN_REGIONS = [
    "Madrid",
    "Barcelona",
    "Valencia",
    "Sevilla",
    "Zaragoza",
    "Málaga",
    "Bilbao",
    "Murcia",
    "Alicante",
    "Córdoba",
    "Granada",
    "Valladolid",
    "Vitoria",
    "Palma de Mallorca",
    "Las Palmas",
]

CATEGORIES = [
    ("Electronics", "Audio"),
    ("Electronics", "Wearables"),
    ("Electronics", "Accesorios"),
    ("Electronics", "Tablets"),
    ("Electronics", "Periféricos"),
    ("Clothing", "Camisetas"),
    ("Clothing", "Chaquetas"),
    ("Clothing", "Pantalones"),
    ("Clothing", "Sudaderas"),
    ("Clothing", "Accesorios"),
    ("Home", "Iluminación"),
    ("Home", "Cocina"),
    ("Home", "Textil"),
    ("Home", "Organización"),
    ("Home", "Decoración"),
    ("Sports", "Fitness"),
    ("Sports", "Hidratación"),
    ("Sports", "Outdoor"),
    ("Sports", "Calzado"),
    ("Books", "Desarrollo Personal"),
    ("Books", "Historia"),
    ("Books", "Tecnología"),
    ("Books", "Cocina"),
    ("Books", "Ficción"),
    ("Books", "Ciencia"),
]

SPANISH_FIRST_NAMES_MALE = [
    "Carlos",
    "Antonio",
    "José",
    "Manuel",
    "Francisco",
    "David",
    "Javier",
    "Daniel",
    "Pablo",
    "Alejandro",
    "Miguel",
    "Jorge",
    "Iván",
    "Óscar",
    "Hugo",
    "Marcos",
    "Raúl",
    "Víctor",
    "Alberto",
    "Pedro",
    "Rafael",
    "Ángel",
    "Eduardo",
    "Ignacio",
    "Sergio",
    "Rubén",
    "Fernando",
    "Diego",
    "Adrián",
    "Lucas",
    "Mario",
    "Alberto",
    "Álvaro",
    "Andrés",
    "Gabriel",
    "Héctor",
    "Iker",
    "Jaime",
    "Jesús",
    "Joel",
    "Jonatan",
    "Kevin",
    "Luis",
    "Marco",
    "Nicolás",
    "Óliver",
    "Pol",
    "Roberto",
    "Samuel",
    "Tomás",
]

SPANISH_FIRST_NAMES_FEMALE = [
    "María",
    "Laura",
    "Ana",
    "Carmen",
    "Isabel",
    "Sofía",
    "Lucía",
    "Marta",
    "Elena",
    "Paula",
    "Andrea",
    "Julia",
    "Clara",
    "Raquel",
    "Nuria",
    "Irene",
    "Beatriz",
    "Sara",
    "Natalia",
    "Teresa",
    "Rocío",
    "Patricia",
    "Cristina",
    "Mónica",
    "Gloria",
    "Silvia",
    "Alicia",
    "Susana",
    "Eva",
    "Pilar",
    "Celia",
    "Aitana",
    "Alba",
    "Alicia",
    "Amelia",
    "Berta",
    "Carla",
    "Diana",
    "Elisa",
    "Emma",
    "Inés",
    "Laia",
    "Lidia",
    "Marina",
    "Nora",
    "Olivia",
    "Paula",
    "Sandra",
    "Valentina",
    "Vega",
]

SPANISH_SURNAMES = [
    "García",
    "Rodríguez",
    "González",
    "Fernández",
    "López",
    "Martínez",
    "Sánchez",
    "Pérez",
    "Gómez",
    "Martín",
    "Jiménez",
    "Ruiz",
    "Hernández",
    "Díaz",
    "Moreno",
    "Álvarez",
    "Muñoz",
    "Romero",
    "Alonso",
    "Gutiérrez",
    "Navarro",
    "Torres",
    "Domínguez",
    "Vázquez",
    "Ramos",
    "Gil",
    "Ramírez",
    "Serrano",
    "Blanco",
    "Molina",
    "Morales",
    "Suárez",
    "Ortega",
    "Delgado",
    "Castro",
    "Ortiz",
    "Rubio",
    "Marín",
    "Sanz",
    "Iglesias",
    "Medina",
    "Garrido",
    "Cortés",
    "Castillo",
    "Santos",
    "Lozano",
    "Guerrero",
    "Cano",
    "Prieto",
    "Méndez",
    "Cruz",
    "Calvo",
    "Gallego",
    "Vidal",
    "León",
    "Herrera",
    "Flores",
    "Cabrera",
    "Campos",
    "Vega",
    "Reyes",
    "Fuentes",
    "Carrasco",
    "Diez",
    "Caballero",
    "Nieto",
    "Aguilar",
    "Pascual",
    "Santana",
    "Herrero",
    "Montero",
    "Lorenzo",
    "Hidalgo",
    "Giménez",
    "Ibáñez",
    "Ferrer",
    "Durán",
    "Santiago",
    "Benítez",
    "Mora",
    "Vicente",
    "Vargas",
    "Arias",
    "Carmona",
    "Crespo",
    "Rojas",
]

PRODUCT_NAME_PARTS = {
    "Electronics": {
        "Audio": [
            "Auriculares Bluetooth",
            "Altavoz Portátil",
            "Cascos ANC",
            "Microfono USB",
            "Barra de Sonido",
        ],
        "Wearables": [
            "Smartwatch Deportivo",
            "Pulsera Fitness",
            "Gafas Inteligentes",
            "Anillo Salud",
        ],
        "Accesorios": [
            "Cargador USB-C",
            "Cable Lightning",
            "Hub USB 3.0",
            "Soporte Monitor",
            "Alfombrilla Gaming",
        ],
        "Tablets": [
            "Tablet Android",
            "iPad Funda",
            "Lápiz Digital",
        ],
        "Periféricos": [
            "Ratón Ergonómico",
            "Teclado Mecánico",
            "Webcam Full HD",
            "Micrófono Condensador",
            "Docking Station",
        ],
    },
    "Clothing": {
        "Camisetas": ["Camiseta Algodón", "Camiseta Manga Larga", "Polo Premium"],
        "Chaquetas": ["Chaqueta Impermeable", "Chaleco Acolchado", "Cortavientos"],
        "Pantalones": ["Pantalón Chino", "Jeans Slim Fit", "Pantalón Jogger"],
        "Sudaderas": ["Sudadera Capucha", "Sudadera Oversize", "Jersey Lana"],
        "Accesorios": ["Calcetines Merino", "Bufanda Alpaca", "Gorro Lana"],
    },
    "Home": {
        "Iluminación": ["Lámpara LED", "Lámpara Mesa", "Tira LED RGB"],
        "Cocina": ["Set Cubiertos", "Sartén Antiadherente", "Batidora Vaso"],
        "Textil": ["Manta Polar", "Cojín Decorativo", "Cortinas Blackout"],
        "Organización": [
            "Organizador Cajón",
            "Estante Flotante",
            "Caja Almacenamiento",
        ],
        "Decoración": ["Reloj Despertador", "Marco Fotos", "Planta Artificial"],
    },
    "Sports": {
        "Fitness": ["Esterilla Yoga", "Bandas Elásticas", "Mancuernas Ajustables"],
        "Hidratación": ["Botella Acero", "Termo Café", "Bolsa Hielo"],
        "Outdoor": ["Mochila Senderismo", "Tienda Campaña", "Linterna Frontal"],
        "Calzado": ["Zapatillas Running", "Sandalias Trekking", "Zapatillas Crossfit"],
    },
    "Books": {
        "Desarrollo Personal": [
            "El Arte de No Amargarse la Vida",
            "Hábitos Atómicos",
            "Mindset",
        ],
        "Historia": ["Sapiens", "Breve Historia del Mundo", "Los Pilares de la Tierra"],
        "Tecnología": [
            "Clean Code",
            "Designing Data-Intensive Apps",
            "The Pragmatic Programmer",
        ],
        "Cocina": ["Cocina Mediterránea", "Recetas Fáciles", "Pan Artesanal"],
        "Ficción": ["La Sombra del Viento", "El Quijote", "Cien Años de Soledad"],
        "Ciencia": ["Breves Respuestas", "El Gen", "Cosmos"],
    },
}

PRICE_RANGES = {
    "Electronics": (990, 49900),
    "Clothing": (990, 14990),
    "Home": (990, 7990),
    "Sports": (990, 12990),
    "Books": (990, 3990),
}


def random_date(start: date, end: date) -> date:
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, delta))


def generate_customers() -> list[dict]:
    customers = []
    for cid in range(1, NUM_CUSTOMERS + 1):
        is_male = random.random() < 0.5
        names = SPANISH_FIRST_NAMES_MALE if is_male else SPANISH_FIRST_NAMES_FEMALE
        first = random.choice(names)
        surname1 = random.choice(SPANISH_SURNAMES)
        surname2 = random.choice(SPANISH_SURNAMES)
        full_name = f"{first} {surname1} {surname2}"
        email_user = f"{first.lower()}.{surname1.lower()}"
        email = f"{email_user}@email.es"
        country = random.choices(
            ["ES", "FR", "DE", "PT", "IT"], weights=[70, 10, 8, 7, 5], k=1
        )[0]
        lang = "es" if country == "ES" else random.choice(["es", "en", "fr"])
        customers.append(
            {
                "customer_id": cid,
                "customer_name": full_name,
                "email": email,
                "country": country,
                "signup_date": random_date(DATE_START, DATE_END).isoformat(),
                "preferred_language": lang,
            }
        )
    return customers


def generate_products() -> list[dict]:
    products = []
    pid = 1
    for category, subcats in PRODUCT_NAME_PARTS.items():
        for subcat, names in subcats.items():
            for base_name in names:
                lo, hi = PRICE_RANGES[category]
                price_cents = random.randrange(lo, hi, 10)
                # Add variant suffix for extra products
                variants = ["", " Pro", " Lite", " Max", " Mini"]
                for variant in variants:
                    if pid > NUM_PRODUCTS:
                        break
                    name = f"{base_name}{variant}"
                    products.append(
                        {
                            "product_id": pid,
                            "product_name": name,
                            "category": category,
                            "subcategory": subcat,
                            "unit_price_cents": price_cents
                            + (random.randint(-200, 200) if variant else 0),
                        }
                    )
                    pid += 1
                if pid > NUM_PRODUCTS:
                    break
            if pid > NUM_PRODUCTS:
                break
        if pid > NUM_PRODUCTS:
            break

    # Fill remaining slots if we didn't reach NUM_PRODUCTS
    while pid <= NUM_PRODUCTS:
        cat = random.choice(list(PRODUCT_NAME_PARTS.keys()))
        subcat = random.choice(list(PRODUCT_NAME_PARTS[cat].keys()))
        base = random.choice(PRODUCT_NAME_PARTS[cat][subcat])
        lo, hi = PRICE_RANGES[cat]
        products.append(
            {
                "product_id": pid,
                "product_name": f"{base} Edición {pid}",
                "category": cat,
                "subcategory": subcat,
                "unit_price_cents": random.randrange(lo, hi, 10),
            }
        )
        pid += 1

    return products


def generate_orders() -> list[dict]:
    orders = []
    for oid in range(1, NUM_ORDERS + 1):
        cust_id = random.randint(1, NUM_CUSTOMERS)
        region = random.choice(SPAIN_REGIONS)
        status = random.choices(
            ["completed", "pending", "cancelled", "refunded"],
            weights=[65, 18, 10, 7],
            k=1,
        )[0]
        orders.append(
            {
                "order_id": oid,
                "customer_id": cust_id,
                "order_date": random_date(DATE_START, DATE_END).isoformat(),
                "status": status,
                "region": region,
                "currency": "EUR",
            }
        )
    return orders


def generate_order_items(orders: list[dict], products: list[dict]) -> list[dict]:
    items = []
    item_id = 1
    product_prices = {p["product_id"]: p["unit_price_cents"] for p in products}

    for order in orders:
        oid = order["order_id"]
        # Poisson-ish number of items, min 1, max 10
        n_items = min(10, max(1, int(random.gauss(AVG_ITEMS_PER_ORDER, 1.2))))
        # Avoid duplicate product within same order
        chosen_products = random.sample(
            list(product_prices.keys()), min(n_items, len(product_prices))
        )
        for pid in chosen_products:
            qty = random.randint(1, 5)
            items.append(
                {
                    "order_item_id": item_id,
                    "order_id": oid,
                    "product_id": pid,
                    "quantity": qty,
                    "unit_price_cents": product_prices[pid],
                }
            )
            item_id += 1
    return items


def write_csv(filename: str, rows: list[dict]) -> None:
    path = SEED_DIR / filename
    if not rows:
        return
    fieldnames = list(rows[0].keys())
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"  {filename}: {len(rows):,} rows → {path}")


def main() -> None:
    print("Generating large seed dataset …\n")

    print(f"  Generating {NUM_CUSTOMERS:,} customers …")
    customers = generate_customers()

    print(f"  Generating {NUM_PRODUCTS:,} products …")
    products = generate_products()

    print(f"  Generating {NUM_ORDERS:,} orders …")
    orders = generate_orders()

    print(f"  Generating order items (~{int(NUM_ORDERS * AVG_ITEMS_PER_ORDER):,}) …")
    order_items = generate_order_items(orders, products)

    print()
    write_csv("raw_customers.csv", customers)
    write_csv("raw_products.csv", products)
    write_csv("raw_orders.csv", orders)
    write_csv("raw_order_items.csv", order_items)

    print(f"\nDone. Files written to {SEED_DIR}/")
    print(f"  Total order items: {len(order_items):,}")


if __name__ == "__main__":
    main()
