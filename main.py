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
import ksuid
from twocaptcha import TwoCaptcha


url = "https://operations-api-service-3hhibg5dra-tl.a.run.app/try-again-connections/get-pending-aguas-andinas"
headers = {
    "Authorization": "Bearer 1234"
}

if not firebase_admin._apps:
    cred = credentials.Certificate("service-account-key.json") 
    firebase_admin.initialize_app(cred)
    
db = firestore.client()

def eliminarCuentaError(nroCuentaString):
    print("Eliminando cuenta mal puesta")
    try:
        driver.get("https://www.aguasandinas.cl/web/aguasandinas/informacion-de-la-cuenta")
        btnAddAccount = wait.until(EC.element_to_be_clickable((By.XPATH, "//a[@id='linkCuenta']//i")))
        btnAddAccount.click()
            
        btnCerrarPopUp = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[@title='Close (Esc)']")))
        btnCerrarPopUp.click()
        
        if wait_for_element(driver, (By.XPATH, "//td[contains(text(),'Eliminar')]")):
            
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
    except:
        driver.get("https://www.aguasandinas.cl/web/aguasandinas/informacion-de-la-cuenta")
        btnAddAccount = wait.until(EC.element_to_be_clickable((By.XPATH, "//a[@id='linkCuenta']//i")))
        btnAddAccount.click()
            
        btnCerrarPopUp = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[@title='Close (Esc)']")))
        btnCerrarPopUp.click()
        
        if wait_for_element(driver, (By.XPATH, "//td[contains(text(),'Eliminar')]")):
            
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
    unit: str,
    reading_date: str,
    month: str,
    year: str,
    type_: str,
    due_date: str,
    total_a_pagar: Optional[float] = None,
    previous_balance: Optional[float] = 0,
    bucket_url: Optional[str] = None,
    pdf: Optional[Any] = None,  # Si es un archivo o bytes, puedes especificar bytes
    invoice_details: Optional[Dict] = None,
    is_factura: Optional[bool] = None
) -> str:
    collection_ref = db.collection("readings")

    # Buscar si ya existe un documento para esa conexión, mes y año
    existing_docs = collection_ref.where("serviceConnectionId", "==", service_connection_id) \
                                  .where("month", "==", month) \
                                  .where("year", "==", year) \
                                  .stream()

    doc_id = next((doc.id for doc in existing_docs), None)

    # Generar ID si no existe documento previo
    new_id = generate_ksuid("read") if not doc_id else doc_id

    reading_data = {
        "id": new_id,
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

    if doc_id:
        collection_ref.document(doc_id).set(reading_data, merge=True)
        print(f"Service reading updated with ID: {doc_id}")
    else:
        collection_ref.document(new_id).set(reading_data)
        print(f"New service reading created with ID: {new_id}")

    return new_id

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
            
            if wait_for_element(driver, (By.XPATH, "//*[contains(text(),'Por favor ingrese su')]")):
                print("Usuario no existe, volviendo al home")
                btnCerrarPopUp = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[@title='Close (Esc)']")))
                btnCerrarPopUp.click()
                
                btnVolverHome = wait.until(EC.element_to_be_clickable((By.XPATH, "//name[contains(text(),'Información de la cuenta')]/../..")))
                btnVolverHome.click()
                
                update_connection_scrap_bad(data["id"], False, "error", "Numero de cliente no existe.")
                return
            
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
            create_service_reading(data["serviceId"],data["id"],data["userId"],float(consumoTotal[i]),"m3",devolverFechaVuelta(fechaLectura[i]).replace('/','-'),devolverMes(fechaLectura[i]),devolverAnio(fechaLectura[i]), "water", devolverFechaVuelta(listaFechaVenc[i]).replace('/','-'), limpiarMonto(listaMonto[i]))
            
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
        
        print(f"Scrapp_de_cliente_{data['config']['clientId']}")
        
    except Exception as e:
        print(f"Error:{e}")
        print("Error durante scrapping, sacando foto")
        tomar_screenshot(driver, carpeta_descarga, f"FallaScrapping_{data['config']['clientId']}.png")
        eliminarCuentaError(nroCuentaString)
        
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
        
def isVisibleCatpchaAndResolve(url):
    if wait_for_element(driver, (By.XPATH, "//iframe[@id = 'main-iframe']")):
        print("Captcha aparecido")
        tomar_screenshot(driver, carpeta_descarga, f"captchaUnResolve_{url}.png")
        solver = TwoCaptcha('959db27aed2dd71809556c892b7d9cb7')
        sitekey = 'dd6e16a7-972e-47d2-93d0-96642fb6d8de'
        try:
            result = solver.hcaptcha(sitekey=sitekey, url=url)
            iframe = driver.find_element(By.XPATH, "//iframe[@id = 'main-iframe']")
            driver.switch_to.frame(iframe)
            recaptchaName = driver.find_element("//textarea[contains(@id,'g-recaptcha-response')]")
            id_elemento = recaptchaName.get_attribute("id")
            driver.execute_script(f'document.getElementById("{id_elemento}").innerHTML="{result["code"]}";')
            btn2Captcha = wait.until(EC.element_to_be_clickable((By.XPATH, "//div[@id='checkbox']")))
            btn2Captcha.click()
            driver.switch_to.default_content()
            tomar_screenshot(driver, carpeta_descarga, f"captchaResolve_{url}.png")
        except Exception as e:
            print("Error al resolver el CAPTCHA:", str(e))
            
def isVisibleLoginAndLogIn():
    if wait_for_element(driver, (By.ID, "rut2")):
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
            tomar_screenshot(driver, carpeta_descarga, "PopUpNoVisible.png")
    

print("Iniciando codigo...")
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
wait = WebDriverWait(driver, 5)

isVisibleCatpchaAndResolve("https://www.aguasandinas.cl/web/aguasandinas/login")

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

isVisibleCatpchaAndResolve("https://www.aguasandinas.cl/web/aguasandinas/login")

try:
    btnCerrar = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[contains(text(),'CERRAR')]")))
    btnCerrar.click()
    
except Exception as e:
    print("PopUp No Visible")
    tomar_screenshot(driver, carpeta_descarga, "PopUpNoVisible.png")

if wait_for_element(driver, (By.XPATH, "//li[contains(text(),'Cuenta N°:')]//span")):
    tomar_screenshot(driver, carpeta_descarga, "LoginExitoso.png")
    while(True):
        try:
            data = obtenerPrimerDato()
            if obtenerPrimerDato() != False:
                scrapping2(data)
            else:
                print("No hay datos, esperando...")
                time.sleep(120)
                btnVolverHome = wait.until(EC.element_to_be_clickable((By.XPATH, "//name[contains(text(),'Información de la cuenta')]/../..")))
                btnVolverHome.click()
                tomar_screenshot(driver, carpeta_descarga, "EsperaExitosa.png")
        except:
                tomar_screenshot(driver, carpeta_descarga, "ErrorDuranteEspera.png")
                isVisibleCatpchaAndResolve("https://www.aguasandinas.cl/web/aguasandinas/informacion-de-la-cuenta")
                isVisibleLoginAndLogIn()
                