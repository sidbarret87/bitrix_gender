import requests
import psycopg2

# Универсальная функция для выполнения REST-запросов
def rest_request(url: str, method: str, data: dict) -> dict:
    """
    Выполняет REST-запрос.

    Args:
        url (str): URL для запроса.
        method (str): Метод запроса.
        data (dict): Данные запроса.

    Returns:
        dict: Результат запроса в формате JSON.
    """
    full_url = f"{url}/{method}.json"
    try:
        response = requests.post(full_url, json=data)
        if response.status_code == 200:
            return response.json()
        else:
            return None
    except Exception as e:
        print("Ошибка при выполнении запроса:", e)
        return None

# Обновление контакта в Битрикс24
def update_contact(url: str, contact_id: str, fields: dict, params: dict):
    """
    Обновляет контакт в Битрикс24.

    Args:
        url (str): URL для запроса.
        contact_id (str): ID контакта.
        fields (dict): Поля для обновления.
        params (dict): Дополнительные параметры.

    Returns:
        dict: Результат запроса в формате JSON.
    """
    method = 'crm.contact.update'
    payload = {
        'id': contact_id,
        'fields': fields,
        'params': params
    }
    return rest_request(url, method, payload)

# Получение списка контактов из Битрикс24
def get_contact_list(url: str) -> dict:
    """
    Получает список контактов из Битрикс24.

    Args:
        url (str): URL для запроса.

    Returns:
        dict: Результат запроса в формате JSON.
    """
    method = 'crm.contact.list'
    return rest_request(url, method, {"select":["ID", "NAME","HONORIFIC"]})

# Получение пола из базы данных по имени
def get_gender_from_db(name):
    """
    Получает пол из базы данных по имени.

    Args:
        name (str): Имя.

    Returns:
        str: Пол (Мужчина/Женщина).
    """
    gender = None
    try:
        connection = psycopg2.connect(
            user = "postgres",
            password = 'leopard',
            host='localhost',
            port='5432',
            database='gender'
        )
        cursor = connection.cursor()
        cursor.execute(f"SELECT 1 FROM names_woman WHERE name = '{name}'")
        if cursor.fetchone():
            gender= 'Женщина'
        else:
            cursor.execute(f"SELECT 1 FROM names_man WHERE name = '{name}'")
            if cursor.fetchone():
                gender = 'Мужчина'
    except (Exception, psycopg2.Error) as error:
        print("Ошибка при работе с PostgreSQL:", error)
    finally:
        if connection:
            cursor.close()
            connection.close()
    return gender

# Обработка входящего запроса по Webhook
def process_webhook(data):
    """
    Обрабатывает входящий запрос по Webhook.

    Args:
        data (dict): Данные запроса.

    Returns:
        None
    """
    result =  data.get('result',[])
    for contact in result:
        contact_id = contact.get('ID')
        contact_name = contact.get('NAME')

        if contact_id and contact_name:
            gender = get_gender_from_db(contact_name)
            if (gender):
                contact_type = 'Мужчина' if gender == 'Мужчина' else 'Женщина'
                fields = {
                    "HONORIFIC": "HNR_RU_1" if gender == 'Мужчина' else "HNR_RU_2"
                }
                params = {'REGISTER_SONET_EVENT': 'Y'}
                response = update_contact(url_webhook, contact_id, fields, params)
                if response:
                    print(f"Данные успешно обновлены в Битрикс24 для контакта {contact_name}")
                else:
                    print(f"Ошибка при обновлении данных в Битрикс24 для контакта {contact_name}")
            else:
                print(f"Имя {contact_name} не найдено в базе данных")

# URL для вебхука
url_webhook = 'https://b24-g0r9n5.bitrix24.ru/rest/1/a1lh50fxerthr58h/'

# Получение списка контактов
contact_list = get_contact_list(url_webhook)
process_webhook(contact_list)
