from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import time
import random
import os
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium_stealth import stealth
import pandas as pd
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
import psutil
from google.cloud import storage
import uvicorn
import signal
import firebase_admin
from firebase_admin import credentials, firestore
from typing import Optional, Dict, Any
import ksuid

app = FastAPI()

if not firebase_admin._apps:
    cred = credentials.Certificate("/root/aguas-andinas/service-account-key.json") 
    firebase_admin.initialize_app(cred)

db = firestore.client()

class ScrappingRequest(BaseModel):
    usuario: str
    password: str

BUCKET_NAME = "service_invoices_dev"

storage_client = storage.Client()
bucket = storage_client.bucket(BUCKET_NAME)

download_directory = os.path.abspath("Archivos")
os.makedirs(download_directory, exist_ok=True)

def update_connection_status(connection_id: str, new_status: str):
    doc_ref = db.collection("connections").document(connection_id)
    doc_ref.update({"status": new_status})
    print(f"Connection {connection_id} status updated to {new_status}")

def update_connection_scrap(connection_id: str, scrapSuccess: bool, status: str):
    doc_ref = db.collection("connections").document(connection_id)
    doc_ref.update({"scrapSuccess": scrapSuccess, "status": status})
    print(f"Connection {connection_id} scrap success updated to {scrapSuccess}")

def generate_ksuid(prefix: str = None) -> str:
    generated_ksuid = ksuid.ksuid() 
    id_string = str(generated_ksuid) 

    if prefix:
        return f"{prefix}_{id_string}"
    return id_string

def create_service_reading(
    service_id: str,
    service_connection_id: str,
    user_id: str,
    value: float,
    status_service: str,
    unit: str,
    reading_date: str,
    month: str,
    year: str,
    type_: str,
    due_date: str,
    total_a_pagar: Optional[float] = None,
    previous_balance: Optional[float] = None,
    bucket_url: Optional[str] = None,
    pdf: Optional[Any] = None,
    invoice_details: Optional[Dict] = None,
    is_factura: Optional[bool] = None
) -> str:
    collection_ref = db.collection("readings")

    existing_docs = collection_ref.where("serviceConnectionId", "==", service_connection_id)\
                                  .where("month", "==", month)\
                                  .where("year", "==", year)\
                                  .stream()
    
    doc_id = None
    for doc in existing_docs:
        doc_id = doc.id 
        break
    
    reading_data = {
        "id": generate_ksuid("read"),
        "serviceId": service_id,
        "serviceConnectionId": service_connection_id,
        "userId": user_id,
        "value": value,
        "totalAPagar": total_a_pagar,
        "previousBalance": previous_balance,
        "statusService": status_service,
        "unit": unit,
        "readingDate": reading_date,
        "month": month,
        "year": year,
        "type": type_,
        "bucketURL": bucket_url,
        "invoiceDetails": invoice_details,
        "isFactura": is_factura,
        "dueDate": due_date,
    }

    if doc_id:
        collection_ref.document(doc_id).set(reading_data, merge=True)
        print(f"Service reading updated with ID: {doc_id}")
    else:
        doc_ref = collection_ref.add(reading_data)
        doc_id = doc_ref[1].id
        print(f"New service reading created with ID: {doc_id}")
    
    return doc_id

def upload_to_gcs(local_path, destination_blob_name):
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(local_path)
    return f"https://storage.googleapis.com/{BUCKET_NAME}/{destination_blob_name}"

def tomar_screenshot(driver, nombre="screenshot.png"):
    ruta_screenshot = os.path.join(download_directory, nombre)
    driver.save_screenshot(ruta_screenshot)
    print(f"Screenshot guardado como {ruta_screenshot}")
    upload_to_gcs(ruta_screenshot, nombre)
    
def cerrar_edge():
    for proc in psutil.process_iter(attrs=['pid', 'name']):
        if proc.info['name'] == "msedge.exe":
            os.kill(proc.info['pid'], signal.SIGTERM)
    
def scrapping(usuario: str, password: str):
    MAX_INTENTOS = 3
    intentos = 0

    while intentos < MAX_INTENTOS:
        try:
            print(f"Intento {intentos + 1} de {MAX_INTENTOS}...")

            # Configurar opciones de Chrome
            options = uc.ChromeOptions()
            options.add_argument("--headless")
            options.add_argument("--disable-gpu")
            options.add_argument("--window-size=1920x1080")
            options.add_argument("--disable-blink-features=AutomationControlled")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--disable-extensions")
            options.add_argument("--disable-popup-blocking")
            options.add_argument("--disable-notifications")
            options.add_argument("--lang=es-ES")
            options.add_argument("--incognito")
            options.add_argument("--blink-settings=imagesEnabled=true")
            options.add_argument("--font-render-hinting=none")
            

            user_agents = [
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
            ]
            options.add_argument(f"user-agent={random.choice(user_agents)}")

            prefs = {"download.default_directory": download_directory}
            options.add_experimental_option("prefs", prefs)

            driver = uc.Chrome(options=options)

            stealth(driver,
                    languages=["es-ES", "en"],
                    vendor="Google Inc.",
                    platform="Win32",
                    webgl_vendor="Intel Inc.",
                    renderer="Intel Iris OpenGL Engine",
                    fix_hairline=True,
                    )

            driver.execute_script("""
                Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                Object.defineProperty(navigator, 'languages', {get: () => ['es-ES', 'en-US']});
                Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
                window.navigator.chrome = { runtime: {} };
                window.chrome = { runtime: {} };
            """)

            driver.execute_script(""" 
                const getParameter = WebGLRenderingContext.prototype.getParameter;
                WebGLRenderingContext.prototype.getParameter = function(parameter) {
                    if (parameter === 37445) { return 'Intel UHD Graphics'; }
                    return getParameter(parameter);
                };
            """)
            driver.delete_all_cookies()
            driver.get("https://www.aguasandinas.cl/web/aguasandinas/login")
            wait = WebDriverWait(driver, 10)
            tomar_screenshot(driver, "login_page.png")

            inputRut = wait.until(EC.presence_of_element_located((By.ID, "rut2")))
            for char in usuario:
                inputRut.send_keys(char)
                time.sleep(random.uniform(1.2, 3.5))

            inputPass = wait.until(EC.presence_of_element_located((By.ID, "clave")))
            for char in password:
                inputPass.send_keys(char)
                time.sleep(random.uniform(1.2, 3.5))

            btnIngresar = wait.until(EC.element_to_be_clickable((By.XPATH, "//input[@value='INGRESAR']")))
            btnIngresar.click()
            
            try:
                btnCerrar = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[contains(text(),'CERRAR')]")))
                btnCerrar.click()
            except Exception as e:
                print("PopUp No Visible")

            tomar_screenshot(driver, "login_page1.png")

            time.sleep(random.uniform(3, 5))
            btnMisCuentas = wait.until(EC.element_to_be_clickable((By.XPATH, "//name[contains(text(),'Mis cuentas')]/../..")))
            btnMisCuentas.click()

            tomar_screenshot(driver, "login_page2.png")

            numerosFactura = []
            mes = []
            fechaVencimiento = []
            monto = []
            estado = []

            for i in range(1, 4):
                numerosFactura.append(driver.find_element(By.XPATH, f'(//td[@data-th="Numero de factura"])[{i}]').text)
                mes.append(driver.find_element(By.XPATH, f'(//td[@data-th="Mes"])[{i}]').text)
                fechaVencimiento.append(driver.find_element(By.XPATH, f'(//td[@data-th="Fecha de vencimiento"])[{i}]').text)
                monto.append(driver.find_element(By.XPATH, f'(//td[@data-th="Monto"])[{i}]').text)
                estado.append(driver.find_element(By.XPATH, f'(//td[@data-th="Estado"])[{i}]').text)
                btnDescargaPDF = driver.find_element(By.XPATH, f'(//td[@data-th="PDF"])[{i}]')
                btnDescargaPDF.click()
                time.sleep(3)
                cerrar_edge()
                time.sleep(1)

            data = {
                "Numero Factura": numerosFactura,
                "Mes": mes,
                "Fecha Vencimiento": fechaVencimiento,
                "Monto": monto,
                "Estado": estado
            }
            
            data = {"Numero Factura": numerosFactura, "Mes": mes, "Fecha Vencimiento": fechaVencimiento, "Monto": monto, "Estado": estado}
            df = pd.DataFrame(data)
            excel_path = os.path.join(download_directory, "facturas.xlsx")
            df.to_excel(excel_path, index=False)
            print(f"Archivo Excel guardado en: {excel_path}")
            upload_to_gcs(excel_path, "facturas.xlsx")

            tomar_screenshot(driver, "login_page4.png")
            driver.quit()
            print("Proceso completado con éxito.")
            break  

        except TimeoutException:
            print("Tiempo de espera agotado esperando un elemento.")
            if 'driver' in locals():
                driver.quit()

            intentos += 1
            if intentos >= MAX_INTENTOS:
                print("Se alcanzó el número máximo de intentos. Saliendo...")
        except NoSuchElementException:
            print("No se encontró un elemento en la página.")
            if 'driver' in locals():
                driver.quit()

            intentos += 1
            if intentos >= MAX_INTENTOS:
                print("Se alcanzó el número máximo de intentos. Saliendo...")
        except WebDriverException as e:
            print(f"Error con el WebDriver: {e}")
            if 'driver' in locals():
                driver.quit()

            intentos += 1
            if intentos >= MAX_INTENTOS:
                print("Se alcanzó el número máximo de intentos. Saliendo...")
        except Exception as e:
            print(f"Error ocurrido: {e}")
            if 'driver' in locals():
                driver.quit()

            intentos += 1
            if intentos >= MAX_INTENTOS:
                print("Se alcanzó el número máximo de intentos. Saliendo...")

@app.get("/")
def read_root():
    return {"message": "Server running!"}

@app.post("/scrap")
async def scrap_data(request: ScrappingRequest):
    """Recibe usuario y contraseña, ejecuta el scraping y devuelve las URLs de los archivos."""
    try:
        urls = scrapping(request.usuario, request.password)
        return {"message": "Scraping exitoso", "urls": urls}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    uvicorn.run(app, host="0.0.0.0", port=port)


