import time
import random
import os
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium_stealth import stealth
import pandas as pd
import psutil
from datetime import datetime
import requests
import firebase_admin
from firebase_admin import credentials, firestore
from typing import Optional, Dict, Any
from datetime import datetime

url = "https://operations-api-service-3hhibg5dra-tl.a.run.app/try-again-connections/get-pending-aguas-andinas"
headers = {
    "Authorization": "Bearer 1234"
}

if not firebase_admin._apps:
    cred = credentials.Certificate("service-account-key.json") 
    firebase_admin.initialize_app(cred)
    
db = firestore.client()

def devolverFechaVuelta(fechaOriginal):
    fecha_str = fechaOriginal
    fecha_obj = datetime.strptime(fecha_str, "%d/%m/%Y")
    fecha_formateada = fecha_obj.strftime("%Y/%m/%d")
    return str(fecha_formateada)

def devolverMes(fechaOriginal):
    fecha_str = fechaOriginal
    fecha_obj = datetime.strptime(fecha_str, "%d/%m/%Y")
    mes = fecha_obj.month
    return str(mes).zfill(2)

def devolverAnio(fechaOriginal):
    fecha_str = fechaOriginal
    fecha_obj = datetime.strptime(fecha_str, "%d/%m/%Y")
    anio = fecha_obj.year
    return str(anio)

def limpiarMonto(montoOriginal):
    valor = montoOriginal
    valor_limpio = valor.replace('$', '').replace('.', '')
    valor_numero = int(valor_limpio)
    return float(valor_numero)
    

def create_service_reading(
    service_id: str,
    service_connection_id: str,
    user_id: str,
    value: float,
    unit: str,
    reading_date: str,
    month: str,
    year: str,
    type_: str,
    due_date: str,
    total_a_pagar: Optional[float] = None,
    previous_balance: Optional[float] = 0,
    bucket_url: Optional[str] = None,
    pdf: Optional[Any] = None,
    invoice_details: Optional[Dict] = None,
    is_factura: Optional[bool] = None
) -> str:
    reading_data = {
        "serviceId": service_id,
        "serviceConnectionId": service_connection_id,
        "userId": user_id,
        "value": value,
        "totalAPagar": total_a_pagar,
        "previousBalance": previous_balance,
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
    doc_ref = db.collection("readings").add(reading_data)
    print(f"New service reading created with ID: {doc_ref[1].id}")
    return doc_ref[1].id
    

def update_scrapping_success(connection_id: str, scrapSuccess: bool, minerSuccess:bool, errorDetail:str = "", status = "active"):
    doc_ref = db.collection("connections").document(connection_id)
    doc_ref.update({"scrapSuccess": scrapSuccess, "minerSuccess":minerSuccess, "errorDetail":errorDetail, "status":status})

def update_connection_scrap_bad(connection_id: str, scrapSuccess: bool, status: str, error:str):
    doc_ref = db.collection("connections").document(connection_id)
    doc_ref.update({"scrapSuccess": scrapSuccess, "status": status, "errorDetail":error})
    print(f"Connection {connection_id} scrap success updated to {scrapSuccess}")
    

download_directory = os.path.abspath("Archivos")
os.makedirs(download_directory, exist_ok=True)

def wait_for_element(driver, locator, timeout=10):
    try:
        WebDriverWait(driver, timeout).until(
            EC.visibility_of_element_located(locator)
        )
        return True
    except:
        return False

def obtenerPrimerDato():
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        data = response.json()
        if(len(data) > 0):
            return data[0]
        else:
            return False
    else:
        print(f"Error: {response.status_code}")

def crear_carpeta_descarga():
    fecha_hora_actual = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    carpeta_descarga = os.path.abspath(f"Archivos/{fecha_hora_actual}")
    os.makedirs(carpeta_descarga, exist_ok=True)
    return carpeta_descarga


def tomar_screenshot(driver, carpeta_descarga, nombre="screenshot.png"):
    ruta_screenshot = os.path.join(carpeta_descarga, nombre)
    driver.save_screenshot(ruta_screenshot)
    print(f"Screenshot guardado como {ruta_screenshot}")


def cerrar_edge():
    for proc in psutil.process_iter(attrs=['pid', 'name']):
        if proc.info['name'] == "msedge.exe":
            os.kill(proc.info['pid'], 9)
    MAX_INTENTOS = 3
    intentos = 0
    
    while intentos < MAX_INTENTOS:
        try:
            print(f"Intento {intentos + 1} de {MAX_INTENTOS}...")


            time.sleep(random.uniform(3, 5))
            btnMisCuentas = wait.until(EC.element_to_be_clickable((By.XPATH, "//name[contains(text(),'Mis cuentas')]/../..")))
            btnMisCuentas.click()

            tomar_screenshot(driver, carpeta_descarga, "login_page2.png")

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
            df = pd.DataFrame(data)
            excel_path = os.path.join(carpeta_descarga, "facturas.xlsx")
            df.to_excel(excel_path, index=False)
            print(f"Archivo Excel guardado en: {excel_path}")

            tomar_screenshot(driver, carpeta_descarga, "login_page4.png")
            time.sleep(1)
            driver.quit()
            print("Proceso completado con éxito.")
            return {"message": "Scraping completado", "excel_path": excel_path}

        except Exception as e:
            print(f"Error ocurrido: {e}")
            if 'driver' in locals():
                driver.quit()

            intentos += 1
            if intentos >= MAX_INTENTOS:
                print("Se alcanzó el número máximo de intentos. Saliendo...")

def scrapping2(data):
    print(f"Probando con el usuario {data['config']['clientId']}")
    try:
        nroCuenta = driver.find_element(By.XPATH,'//li[contains(text(),"Cuenta N°:")]//span')
        nroCuentaString = nroCuenta.text
        
        if(data['config']['clientId'] != nroCuentaString):
            btnAddAccount = wait.until(EC.element_to_be_clickable((By.XPATH, "//a[@id='linkCuenta']//i")))
            btnAddAccount.click()
            
            btnNroCuenta = wait.until(EC.element_to_be_clickable((By.XPATH, "//input[@id='busqueda_n_cuenta']")))
            btnNroCuenta.click()
            
            inputNroCuenta = wait.until(EC.element_to_be_clickable((By.XPATH, "//input[@id='buscador_cuenta']")))
            inputNroCuenta.send_keys(data['config']['clientId'])
            
            btnBuscar = wait.until(EC.element_to_be_clickable((By.XPATH, "(//input[@value = 'Buscar'])[1]")))
            btnBuscar.click()
            
            if wait_for_element(driver, (By.XPATH, "//p[contains(text(),'No existe la cuenta')]")):
                print("Usuario no existe, volviendo al home")
                btnCerrarPopUp = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[@title='Close (Esc)']")))
                btnCerrarPopUp.click()
                
                btnVolverHome = wait.until(EC.element_to_be_clickable((By.XPATH, "//name[contains(text(),'Información de la cuenta')]/../..")))
                btnVolverHome.click()
                
                update_connection_scrap_bad(data["id"], False, "error", "Numero de cliente no existe.")
                return
                
            else:
                btnSeleccionarCuenta = wait.until(EC.element_to_be_clickable((By.XPATH, "//td[@data-th='Seleccionar']//input")))
                btnSeleccionarCuenta.click()
                
                inputAlias = wait.until(EC.element_to_be_clickable((By.XPATH, "//input[@id='alias']")))
                inputAlias.send_keys("Eliminar")
                
                btnAgregarAccount = wait.until(EC.element_to_be_clickable((By.XPATH, "(//input[@value = 'Agregar cuenta'])[1]")))
                btnAgregarAccount.click()
                
                btnSeleccionarAccount = wait.until(EC.element_to_be_clickable((By.XPATH, "//td[contains(text(),'Eliminar')]/preceding-sibling::*[1]//input")))
                btnSeleccionarAccount.click()
                
                btnGuardarCambios = wait.until(EC.element_to_be_clickable((By.XPATH, "(//a[@id='agregarCuentas'])[1]/following-sibling::*[2]")))
                btnGuardarCambios.click()
                
                btnVolverHome = wait.until(EC.element_to_be_clickable((By.XPATH, "//name[contains(text(),'Información de la cuenta')]/../..")))
                btnVolverHome.click()
                
        btnMisCuentas = wait.until(EC.element_to_be_clickable((By.XPATH, "//name[contains(text(),'Mis cuentas')]/../..")))
        btnMisCuentas.click()
            
        wait_for_element(driver, (By.XPATH, "(//td[@data-th='Numero de factura'])[1]"))
            
            
        meses  = driver.find_elements("xpath", '//td[@data-th="Mes"]')
        if len(meses) > 12:
            elemento = driver.find_element("xpath", "(//td[@data-th='Mes'])[13]/..")
            driver.execute_script("arguments[0].style.display = 'block';", elemento)
            
        meses  = driver.find_elements("xpath", '//td[@data-th="Mes"]')   
        listaMeses = [mes.text for mes in meses]
        listaMeses = listaMeses[:13]
            
        fechaVenc  = driver.find_elements("xpath", '//td[@data-th="Fecha de vencimiento"]')
        listaFechaVenc = [dato.text for dato in fechaVenc]
        listaFechaVenc = listaFechaVenc[:13]
        

        monto  = driver.find_elements("xpath", '//td[@data-th="Monto"]')
        listaMonto = [dato.text for dato in monto]
        listaMonto = listaMonto[:13]


        btnMisConsumos = wait.until(EC.element_to_be_clickable((By.XPATH, "//name[contains(text(),'Mis consumos')]/../..")))
        btnMisConsumos.click()
            
        wait_for_element(driver, (By.XPATH, "(//td[@data-th='Fecha de lectura'])[1]"))
            
        fechaLectura = []
        consumoTotal = []
            
        for i in range(0,len(listaMeses)):
            dato = driver.find_element(By.XPATH,f'//td[contains(text(),"{listaMeses[i]}")]/following-sibling::td[1]')
            fechaLectura.append(dato.text)
            dato = driver.find_element(By.XPATH,f'//td[contains(text(),"{listaMeses[i]}")]/following-sibling::td[4]')
            consumoTotal.append(dato.text)
         
        for i in range(0,len(listaMeses)):
            create_service_reading(data["serviceId"],data["id"],data["userId"],float(consumoTotal[i]),"m3",devolverFechaVuelta(fechaLectura[i]),devolverMes(fechaLectura[i]),devolverAnio(fechaLectura[i]), "water", devolverFechaVuelta(listaFechaVenc[i]), limpiarMonto(listaMonto[i]))
            
        update_scrapping_success(data["id"], True, True)
        
        time.sleep(3)
        if(data['config']['clientId'] == nroCuentaString):
            btnVolverHome = wait.until(EC.element_to_be_clickable((By.XPATH, "//name[contains(text(),'Información de la cuenta')]/../..")))
            btnVolverHome.click()
            return
        
        time.sleep(1)
        
        btnAddAccount = wait.until(EC.element_to_be_clickable((By.XPATH, "//a[@id='linkCuenta']//i")))
        btnAddAccount.click()
        
        btnCerrarPopUp = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[@title='Close (Esc)']")))
        btnCerrarPopUp.click()
        
        btnSeleccionarAccount = wait.until(EC.element_to_be_clickable((By.XPATH, f"//td[contains(text(),'{nroCuentaString}')]/preceding-sibling::*[1]//input")))
        btnSeleccionarAccount.click()
        
        time.sleep(1)
        
        btnGuardarCambios = wait.until(EC.element_to_be_clickable((By.XPATH, "(//a[@id='agregarCuentas'])[1]/following-sibling::*[2]")))
        btnGuardarCambios.click()
        
        btnEliminar = wait.until(EC.element_to_be_clickable((By.XPATH, "//td[contains(text(),'Eliminar')]/following-sibling::*[4]")))
        btnEliminar.click()
        
        btnConfirmarEliminar = wait.until(EC.element_to_be_clickable((By.XPATH, "//a[contains(text(),'Confirmar')]")))
        btnConfirmarEliminar.click()
        
        time.sleep(3)
        
        btnVolverHome = wait.until(EC.element_to_be_clickable((By.XPATH, "//name[contains(text(),'Información de la cuenta')]/../..")))
        btnVolverHome.click()
        
        
    except Exception as e:
        print(f"Error:{e}")
        print("Error durante scrapping, sacando foto")
        tomar_screenshot(driver, carpeta_descarga, "FallaScrapping.png")
        driver.get("https://www.aguasandinas.cl/web/aguasandinas/informacion-de-la-cuenta")
        
def enviar_captcha(api_key, site_key, url):
    response = requests.post("https://2captcha.com/demo/recaptcha-v2", data={
        'key': api_key,
        'method': 'userrecaptcha',
        'googlekey': site_key,
        'pageurl': url,
        'json': 1
    })
    request_id = response.json()['request']
    print("request_id")
    return request_id

def obtener_resultado(api_key, request_id):
    url = f"http://2captcha.com/res.php?key={api_key}&action=get&id={request_id}&json=1"
    while True:
        res = requests.get(url)
        resultado = res.json()
        if resultado['status'] == 1:
            return resultado['request']
        time.sleep(5)  # esperar y reintentar    

carpeta_descarga = crear_carpeta_descarga()
options = uc.ChromeOptions()
#options.add_argument("--headless")
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
prefs = {
    "download.default_directory": carpeta_descarga,
    "plugins.always_open_pdf_externally": True,
    "plugins.plugins_disabled": ["Chrome PDF Viewer"],
    "download.prompt_for_download": False,
    "download.directory_upgrade": True,
    "safebrowsing.enabled": True,
    "profile.default_content_settings.popups": 0,
    "download.open_pdf_in_system_reader": True,
    "profile.managed_default_content_settings.pdf": 2,
    "safebrowsing.disable_download_protection": True
}
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

if wait_for_element(driver, (By.XPATH, "//iframe[@id = 'main-iframe']")):
    print("Captcha aparecido")
    iframe = driver.find_element(By.XPATH, "//iframe[@id = 'main-iframe']")
    driver.switch_to.frame(iframe)
    SITEKEY = "dd6e16a7-972e-47d2-93d0-96642fb6d8de"
    url = "https://www.aguasandinas.cl/web/aguasandinas/login"
    APIKEY = "959db27aed2dd71809556c892b7d9cb7"
    request_id = enviar_captcha(APIKEY, SITEKEY, url)
    print("Esperando respuesta...")
    respuesta_captcha = obtener_resultado(APIKEY, request_id)
    print("Respuesta captcha:", respuesta_captcha)
    driver.execute_script(f'document.getElementById("g-recaptcha-response").innerHTML="{respuesta_captcha}";')
    btn2Captcha = wait.until(EC.element_to_be_clickable((By.XPATH, "//div[@id='checkbox']")))
    btn2Captcha.click()
    driver.switch_to.default_content()

    
tomar_screenshot(driver, carpeta_descarga, "login_page.png")
usuario="19999867-6"
password = "Bmatch12"
inputRut = wait.until(EC.presence_of_element_located((By.ID, "rut2")))
for char in usuario:
    inputRut.send_keys(char)
    time.sleep(random.uniform(0.7, 0.8))

inputPass = wait.until(EC.presence_of_element_located((By.ID, "clave")))
for char in password:
    inputPass.send_keys(char)
    time.sleep(random.uniform(0.7, 0.8))

btnIngresar = wait.until(EC.element_to_be_clickable((By.XPATH, "//input[@value='INGRESAR']")))
btnIngresar.click()

try:
    btnCerrar = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[contains(text(),'CERRAR')]")))
    btnCerrar.click()
    
except Exception as e:
    print("PopUp No Visible")
    
tomar_screenshot(driver, carpeta_descarga, "loginExitoso.png")


while(True):
    data = obtenerPrimerDato()
    if obtenerPrimerDato() != False:
        scrapping2(data)
    else:
        time.sleep(120)
        print("No hay datos, apretando informacion de la cuenta")
        btnVolverHome = wait.until(EC.element_to_be_clickable((By.XPATH, "//name[contains(text(),'Información de la cuenta')]/../..")))
        btnVolverHome.click()