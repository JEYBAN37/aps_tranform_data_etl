
def contiene_keyword_indice(paginas: list, keywords: list) -> tuple:
    """
    Recibe lista de (num_pagina, texto).
    Retorna (True, prefijo, num_pagina) o (False, None, None)
    """
    import unicodedata, re

    def normalizar(t):
        t = t.upper()
        t = unicodedata.normalize("NFD", t)
        t = "".join(c for c in t if unicodedata.category(c) != "Mn")
        return t

    for num_pagina, texto in paginas:
        for keyword, prefijo in keywords:
            patron = re.compile(re.escape(normalizar(keyword)))

            for match in patron.finditer(normalizar(texto)):
                fragmento_original = texto[match.start():match.end()]

                if any(c.islower() for c in fragmento_original):
                    continue

                return True, prefijo, num_pagina  # ← retorna qué página

    return False, None, None