import azure.functions as func
import logging
import json
import time
import pandas as pd
import os  # NUEVO: Necesario para leer variables de entorno
import io  # NUEVO: Necesario para BytesIO
from bs4 import BeautifulSoup
from curl_cffi import requests
from datetime import datetime
from azure.storage.blob import BlobServiceClient

app = func.FunctionApp()

def encontrar_resultados(obj):
    if isinstance(obj, dict):
        if "results" in obj and isinstance(obj["results"], list):
            if len(obj["results"]) > 0 and "productId" in obj["results"][0]:
                return obj["results"]
        for v in obj.values():
            res = encontrar_resultados(v)
            if res: return res
    elif isinstance(obj, list):
        for i in obj:
            res = encontrar_resultados(i)
            if res: return res
    return None

def scrap_tecnologia_falabella_infinito():
    base_url = "https://www.falabella.com.pe/falabella-pe/category/CATG46221/Laptops-Gamer?f.product.L3_category_paths=cat40793%7C%7CTecnolog%C3%ADa%2FCATG34760%7C%7CZona+Gamer%2Fcat13000461%7C%7CComputaci%C3%B3n+gamer%2FCATG46221%7C%7CLaptops+Gamer"
    todos_los_productos = []
    page = 1
    continuar = True
    
    while continuar:
        url = f"{base_url}&page={page}"
        logging.info(f"Extrayendo página {page}...")
        
        try:
            response = requests.get(
                url, impersonate="chrome110",
                headers={
                    "Accept-Language": "es-PE,es;q=0.9",
                    "Referer": "https://www.google.com/"
                },
                timeout=30
            )

            if response.status_code != 200:
                logging.warning(f"Status {response.status_code} en página {page}. Deteniendo.")
                break

            soup = BeautifulSoup(response.text, "html.parser")
            script_tag = soup.find("script", id="__NEXT_DATA__")

            if not script_tag:
                logging.info(f"No se encontró más data en página {page}. Fin del scraping.")
                break

            data = json.loads(script_tag.string)
            productos_pagina = encontrar_resultados(data)

            # Validación de parada: Si la lista está vacía, no hay más productos
            if not productos_pagina or len(productos_pagina) == 0:
                logging.info(f"Catálogo terminado en página {page}.")
                continuar = False
                break

            for p in productos_pagina:
                precios_dict = {"CMR": None, "Internet": None, "Normal": None}
                for pr in p.get("prices", []):
                    tipo = pr.get("type", "").lower()
                    try:
                        valor = float(pr.get('price', [0])[0].replace(',', ''))
                    except:
                        valor = None
                    
                    if "cmr" in tipo: precios_dict["CMR"] = valor
                    elif "internet" in tipo or "event" in tipo: precios_dict["Internet"] = valor
                    elif "normal" in tipo: precios_dict["Normal"] = valor

                # Limpieza de porcentaje de descuento
                badge = p.get("discountBadge")
                porcentaje_limpio = None
                if badge and 'label' in badge:
                    try:
                        porcentaje_limpio = int(badge['label'].replace('-', '').replace('%', '').strip())
                    except:
                        pass

                todos_los_productos.append({
                    "Marca": p.get("brand"),
                    "Nombre": p.get("displayName"),
                    "Precio_CMR": precios_dict["CMR"],
                    "Precio_Internet": precios_dict["Internet"],
                    "Precio_Normal": precios_dict["Normal"],
                    "Descuento_Oficial": porcentaje_limpio,
                    "ID_Producto": p.get("productId"),
                    "Fecha_Extraccion": datetime.now().strftime("%Y-%m-%d")
                })
            
            page += 1
            time.sleep(2) # Pausa para evitar bloqueos por rate limiting

        except Exception as e:
            logging.error(f"Error crítico en página {page}: {e}")
            break
            
    return pd.DataFrame(todos_los_productos)

@app.route(route="extract_to_blob", methods=["POST", "GET"])
def extract_to_blob(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Iniciando scraping solicitado por ADF.')

    # 1. Ejecutar Scraping
    df = scrap_tecnologia_falabella_infinito()
    
    if df.empty:
        return func.HttpResponse("No se encontraron productos.", status_code=204)

    try:
        # 2. Configurar conexión a Blob Storage de forma segura
        # Lee la cadena de conexión desde las variables de entorno de Azure
        connection_string = os.environ.get("STORAGE_CONN_STRING")
        
        if not connection_string:
            logging.error("No se encontró la variable de entorno STORAGE_CONN_STRING")
            return func.HttpResponse("Error de configuración interna del servidor.", status_code=500)

        container_name = "falabella-input"
        
        # Generar nombre de archivo único
        fecha_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        blob_name = f"falabella/laptops_{fecha_str}.parquet"

        # 3. Convertir DataFrame a Parquet en memoria
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, engine='pyarrow', index=False)
        parquet_buffer.seek(0)

        # 4. Subir a Azure Blob Storage
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        
        blob_client.upload_blob(parquet_buffer, overwrite=True)

        # 5. Respuesta para Data Factory
        response_body = {
            "status": "success",
            "productos_total": len(df),
            "archivo_generado": blob_name,
            "container": container_name
        }

        return func.HttpResponse(
            json.dumps(response_body),
            mimetype="application/json",
            status_code=200
        )

    except Exception as e:
        logging.error(f"Error al guardar en Blob: {str(e)}")
        return func.HttpResponse(f"Error interno: {str(e)}", status_code=500)