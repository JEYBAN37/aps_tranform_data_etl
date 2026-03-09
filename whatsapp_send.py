import os

import keyboard
import pyautogui
import pywhatkit
import time
import random

base_dir = os.path.dirname(os.path.abspath(__file__))
ruta_imagen = os.path.join(base_dir, "cache", "wp.jpeg")

# 1. Lista de contactos (Número con código de país +57 para Colombia)
# Puedes agregar a todas las jefes aquí
contactos = [
    {"nombre": "Aura", "numero": "+573172960636"},
    {"nombre": "Angela", "numero": "+573015654862"},
    {"nombre": "Sara", "numero": "+573146161960"},
    {"nombre": "Diana", "numero": "+573226512049"},
    {"nombre": "Eduardo", "numero": "+573008345731"},
    {"nombre": "Aura", "numero": "+573235468455"},
    {"nombre": "Jenifer", "numero": "+573126209891"},
    {"nombre": "Antony", "numero": "+573044494900"},
    {"nombre": "Diana", "numero": "+573157839398"},
    {"nombre": "Karol", "numero": "+573122916355"},
    {"nombre": "Maria", "numero": "+573145313736"},
    {"nombre": "Yamile", "numero": "+573107267994"},
    {"nombre": "Carina", "numero": "+573336443928"},
    {"nombre": "Nicol", "numero": "+573502664638"},
    {"nombre": "Yonatan", "numero": "+573167278142"},
    {"nombre": "Patricia", "numero": "+573127266560"},
    {"nombre": "Adriana", "numero": "+573226559694"},
    {"nombre": "Yuliana", "numero": "+573233884894"},
    {"nombre": "Genith", "numero": "+573127302710"},
    {"nombre": "Nuvi", "numero": "+573162960938"},
    {"nombre": "July", "numero": "+573226666890"},
    {"nombre": "Martha", "numero": "+573147111668"},
    {"nombre": "John", "numero": "+573226822803"},
    {"nombre": "Sharon", "numero": "+573137730252"},
    {"nombre": "Evelin", "numero": "+573185353199"},
    {"nombre": "Jineth", "numero": "+573116127840"},
    {"nombre": "Paola", "numero": "+573115366561"},
    {"nombre": "Evelyn", "numero": "+573153719272"},
    {"nombre": "Anyie", "numero": "+573135890726"},
    {"nombre": "Luzdari", "numero": "+573117597491"},
    {"nombre": "Anyely", "numero": "+573195306642"},
    {"nombre": "Adriana", "numero": "+573216664179"},
    {"nombre": "Maribel", "numero": "+573215484255"},
    {"nombre": "Edith", "numero": "+573186418579"},
    {"nombre": "Vivian", "numero": "+573104491036"},
    {"nombre": "Wendy", "numero": "+573126989615"},
    {"nombre": "Ingrid", "numero": "+573122529703"},
    {"nombre": "Lina", "numero": "+573178711561"},
    {"nombre": "Lady", "numero": "+573166076060"},
    {"nombre": "Paola", "numero": "+573003370688"},
    {"nombre": "Vanessa", "numero": "+573117181835"},
    {"nombre": "Rocio", "numero": "+573176246297"},
    {"nombre": "Angie", "numero": "+573153533250"},
    {"nombre": "Martha", "numero": "+573152341929"},
    {"nombre": "Esteban", "numero": "+573177722509"},
]


def crear_vcard(lista_contactos, nombre_archivo="contactos_equipo.vcf"):
    with open(nombre_archivo, "w", encoding="utf-8") as f:
        for persona in lista_contactos:
            f.write("BEGIN:VCARD\n")
            f.write("VERSION:3.0\n")
            f.write(f"FN:{persona['nombre']}\n")
            f.write(f"TEL;TYPE=CELL:{persona['numero']}\n")
            f.write("END:VCARD\n")

    print(f"✅ Archivo '{nombre_archivo}' creado con éxito.")


crear_vcard(contactos)