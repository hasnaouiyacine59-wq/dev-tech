VERSION = "6.0.0"

import random, os, string, sys, json
import argparse
import atexit
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from playwright.sync_api import sync_playwright
import time
import user_agnt
from xvfbwrapper import Xvfb

BASE_DIR        = os.path.dirname(os.path.abspath(__file__))
PROXY_FILE      = os.path.join(BASE_DIR, "proxies.txt")
USED_PROXY_FILE = os.path.join(BASE_DIR, "used_proxies.txt")
LOG_FILE        = os.path.expanduser("~/logs/ip_log.txt")
REQUEST_TIMEOUT = 15

def _test_proxy(proxy: str, timeout: int) -> bool:
    for url in ["http://api.ipify.org", "http://httpbin.org/ip", "http://ifconfig.me/ip"]:
        try:
            r = requests.get(url, proxies={"http": proxy, "https": proxy}, timeout=timeout)
            if r.status_code == 200:
                return True
        except Exception:
            continue
    return False

def get_working_proxy(proxy_list, timeout=REQUEST_TIMEOUT) -> str:
    random.shuffle(proxy_list)
    print(f"[~] Testing {len(proxy_list)} proxies...")
    working = []
    with ThreadPoolExecutor(max_workers=min(len(proxy_list), 20)) as ex:
        futures = {ex.submit(_test_proxy, p, timeout): p for p in proxy_list}
        for f in as_completed(futures):
            if f.result():
                working.append(futures[f])
    if not working:
        raise RuntimeError("No working proxies available!")
    chosen = random.choice(working)
    print(f"✅ {len(working)} working | 🎯 Selected: {chosen}")
    return chosen

def mark_proxy_used(proxy: str):
    with open(PROXY_FILE) as f:
        lines = [l.strip() for l in f if l.strip()]
    remaining = [l for l in lines if not (l == proxy or f"http://{l}" == proxy or l == proxy.replace("http://", ""))]
    with open(PROXY_FILE, "w") as f:
        f.write("\n".join(remaining) + ("\n" if remaining else ""))
    with open(USED_PROXY_FILE, "a") as f:
        f.write(proxy + "\n")
    print(f"[~] Proxy moved to used: {proxy}")

def get_ip_info(proxy_url: str = None, retries: int = 6, delay: int = 5) -> dict:
    proxies = {"http": proxy_url, "https": proxy_url} if proxy_url else None
    urls = ["http://ipwho.is/", "http://ip-api.com/json", "http://api.ipify.org?format=json"]
    for attempt in range(retries):
        for url in urls:
            try:
                r = requests.get(url, timeout=10, proxies=proxies)
                data = r.json()
                if data.get("ip") or data.get("query"):
                    data.setdefault("ip", data.get("query"))
                    data.setdefault("country_code", data.get("countryCode", "US"))
                    return data
            except Exception as e:
                print(f"[-] IP lookup failed ({url}): {e}")
        print(f"[~] Tor not ready yet, retrying in {delay}s... ({attempt+1}/{retries})")
        time.sleep(delay)
    return {}

CC_LANG = {
    "US": ("en-US", "America/New_York",      "en-US,en;q=0.9"),
    "GB": ("en-GB", "Europe/London",         "en-GB,en;q=0.9"),
    "IT": ("it-IT", "Europe/Rome",           "it-IT,it;q=0.9,en;q=0.8"),
    "DE": ("de-DE", "Europe/Berlin",         "de-DE,de;q=0.9,en;q=0.8"),
    "FR": ("fr-FR", "Europe/Paris",          "fr-FR,fr;q=0.9,en;q=0.8"),
    "ES": ("es-ES", "Europe/Madrid",         "es-ES,es;q=0.9,en;q=0.8"),
    "NL": ("nl-NL", "Europe/Amsterdam",      "nl-NL,nl;q=0.9,en;q=0.8"),
    "PL": ("pl-PL", "Europe/Warsaw",         "pl-PL,pl;q=0.9,en;q=0.8"),
    "BR": ("pt-BR", "America/Sao_Paulo",     "pt-BR,pt;q=0.9,en;q=0.8"),
    "RU": ("ru-RU", "Europe/Moscow",         "ru-RU,ru;q=0.9,en;q=0.8"),
    "TR": ("tr-TR", "Europe/Istanbul",       "tr-TR,tr;q=0.9,en;q=0.8"),
    "JP": ("ja-JP", "Asia/Tokyo",            "ja-JP,ja;q=0.9,en;q=0.8"),
    "CN": ("zh-CN", "Asia/Shanghai",         "zh-CN,zh-Hans;q=0.9,en;q=0.8"),
    "SE": ("sv-SE", "Europe/Stockholm",      "sv-SE,sv;q=0.9,en;q=0.8"),
    "MX": ("es-MX", "America/Mexico_City",   "es-MX,es;q=0.9,en;q=0.8"),
    "IN": ("en-IN", "Asia/Kolkata",          "en-IN,en;q=0.9,hi;q=0.8"),
    "AU": ("en-AU", "Australia/Sydney",      "en-AU,en;q=0.9"),
    "CA": ("en-CA", "America/Toronto",       "en-CA,en;q=0.9,fr;q=0.8"),
    "AR": ("es-AR", "America/Argentina/Buenos_Aires", "es-AR,es;q=0.9,en;q=0.8"),
    "UA": ("uk-UA", "Europe/Kyiv",           "uk-UA,uk;q=0.9,en;q=0.8"),
    "RO": ("ro-RO", "Europe/Bucharest",      "ro-RO,ro;q=0.9,en;q=0.8"),
    "HU": ("hu-HU", "Europe/Budapest",       "hu-HU,hu;q=0.9,en;q=0.8"),
    "CZ": ("cs-CZ", "Europe/Prague",         "cs-CZ,cs;q=0.9,en;q=0.8"),
    "PT": ("pt-PT", "Europe/Lisbon",         "pt-PT,pt;q=0.9,en;q=0.8"),
    "GR": ("el-GR", "Europe/Athens",         "el-GR,el;q=0.9,en;q=0.8"),
    "ID": ("id-ID", "Asia/Jakarta",          "id-ID,id;q=0.9,en;q=0.8"),
    "TH": ("th-TH", "Asia/Bangkok",          "th-TH,th;q=0.9,en;q=0.8"),
    "VN": ("vi-VN", "Asia/Ho_Chi_Minh",      "vi-VN,vi;q=0.9,en;q=0.8"),
    "PH": ("en-PH", "Asia/Manila",           "en-PH,en;q=0.9,fil;q=0.8"),
    "ZA": ("en-ZA", "Africa/Johannesburg",   "en-ZA,en;q=0.9"),
    "NG": ("en-NG", "Africa/Lagos",          "en-NG,en;q=0.9"),
    "EG": ("ar-EG", "Africa/Cairo",          "ar-EG,ar;q=0.9,en;q=0.8"),
    "SA": ("ar-SA", "Asia/Riyadh",           "ar-SA,ar;q=0.9,en;q=0.8"),
    "IL": ("he-IL", "Asia/Jerusalem",        "he-IL,he;q=0.9,en;q=0.8"),
    "KR": ("ko-KR", "Asia/Seoul",            "ko-KR,ko;q=0.9,en;q=0.8"),
    "SG": ("en-SG", "Asia/Singapore",        "en-SG,en;q=0.9,zh;q=0.8"),
    "MY": ("ms-MY", "Asia/Kuala_Lumpur",     "ms-MY,ms;q=0.9,en;q=0.8"),
    "NO": ("nb-NO", "Europe/Oslo",           "nb-NO,nb;q=0.9,no;q=0.8,en;q=0.7"),
    "FI": ("fi-FI", "Europe/Helsinki",       "fi-FI,fi;q=0.9,en;q=0.8"),
    "DK": ("da-DK", "Europe/Copenhagen",     "da-DK,da;q=0.9,en;q=0.8"),
    "CH": ("de-CH", "Europe/Zurich",         "de-CH,de;q=0.9,en;q=0.8"),
    "AT": ("de-AT", "Europe/Vienna",         "de-AT,de;q=0.9,en;q=0.8"),
    "BE": ("fr-BE", "Europe/Brussels",       "fr-BE,fr;q=0.9,nl;q=0.8,en;q=0.7"),
    # Africa
    "MA": ("ar-MA", "Africa/Casablanca",     "ar-MA,ar;q=0.9,fr;q=0.8,en;q=0.7"),
    "DZ": ("ar-DZ", "Africa/Algiers",        "ar-DZ,ar;q=0.9,fr;q=0.8,en;q=0.7"),
    "TN": ("ar-TN", "Africa/Tunis",          "ar-TN,ar;q=0.9,fr;q=0.8,en;q=0.7"),
    "KE": ("en-KE", "Africa/Nairobi",        "en-KE,en;q=0.9,sw;q=0.8"),
    "GH": ("en-GH", "Africa/Accra",          "en-GH,en;q=0.9"),
    "ET": ("am-ET", "Africa/Addis_Ababa",    "am-ET,am;q=0.9,en;q=0.8"),
    "TZ": ("sw-TZ", "Africa/Dar_es_Salaam",  "sw-TZ,sw;q=0.9,en;q=0.8"),
    "SN": ("fr-SN", "Africa/Dakar",          "fr-SN,fr;q=0.9,en;q=0.8"),
    "CI": ("fr-CI", "Africa/Abidjan",        "fr-CI,fr;q=0.9,en;q=0.8"),
    "CM": ("fr-CM", "Africa/Douala",         "fr-CM,fr;q=0.9,en;q=0.8"),
    # Middle East
    "AE": ("ar-AE", "Asia/Dubai",            "ar-AE,ar;q=0.9,en;q=0.8"),
    "IQ": ("ar-IQ", "Asia/Baghdad",          "ar-IQ,ar;q=0.9,en;q=0.8"),
    "IR": ("fa-IR", "Asia/Tehran",           "fa-IR,fa;q=0.9,en;q=0.8"),
    "KW": ("ar-KW", "Asia/Kuwait",           "ar-KW,ar;q=0.9,en;q=0.8"),
    "QA": ("ar-QA", "Asia/Qatar",            "ar-QA,ar;q=0.9,en;q=0.8"),
    "JO": ("ar-JO", "Asia/Amman",            "ar-JO,ar;q=0.9,en;q=0.8"),
    "LB": ("ar-LB", "Asia/Beirut",           "ar-LB,ar;q=0.9,fr;q=0.8,en;q=0.7"),
    # Asia
    "PK": ("ur-PK", "Asia/Karachi",          "ur-PK,ur;q=0.9,en;q=0.8"),
    "BD": ("bn-BD", "Asia/Dhaka",            "bn-BD,bn;q=0.9,en;q=0.8"),
    "LK": ("si-LK", "Asia/Colombo",          "si-LK,si;q=0.9,en;q=0.8"),
    "NP": ("ne-NP", "Asia/Kathmandu",        "ne-NP,ne;q=0.9,en;q=0.8"),
    "MM": ("my-MM", "Asia/Yangon",          "my-MM,my;q=0.9,en;q=0.8"),
    "KH": ("km-KH", "Asia/Phnom_Penh",       "km-KH,km;q=0.9,en;q=0.8"),
    "HK": ("zh-HK", "Asia/Hong_Kong",        "zh-HK,zh-Hant;q=0.9,en;q=0.8"),
    "TW": ("zh-TW", "Asia/Taipei",           "zh-TW,zh-Hant;q=0.9,en;q=0.8"),
    "MN": ("mn-MN", "Asia/Ulaanbaatar",      "mn-MN,mn;q=0.9,en;q=0.8"),
    "UZ": ("uz-UZ", "Asia/Tashkent",         "uz-UZ,uz;q=0.9,ru;q=0.8,en;q=0.7"),
    "KZ": ("kk-KZ", "Asia/Almaty",           "kk-KZ,kk;q=0.9,ru;q=0.8,en;q=0.7"),
    "AZ": ("az-AZ", "Asia/Baku",             "az-AZ,az;q=0.9,ru;q=0.8,en;q=0.7"),
    "GE": ("ka-GE", "Asia/Tbilisi",          "ka-GE,ka;q=0.9,en;q=0.8"),
    "AM": ("hy-AM", "Asia/Yerevan",          "hy-AM,hy;q=0.9,en;q=0.8"),
    # Europe (extra)
    "SK": ("sk-SK", "Europe/Bratislava",     "sk-SK,sk;q=0.9,en;q=0.8"),
    "SI": ("sl-SI", "Europe/Ljubljana",      "sl-SI,sl;q=0.9,en;q=0.8"),
    "HR": ("hr-HR", "Europe/Zagreb",         "hr-HR,hr;q=0.9,en;q=0.8"),
    "RS": ("sr-RS", "Europe/Belgrade",       "sr-RS,sr-Cyrl;q=0.9,en;q=0.8"),
    "BG": ("bg-BG", "Europe/Sofia",          "bg-BG,bg;q=0.9,en;q=0.8"),
    "LT": ("lt-LT", "Europe/Vilnius",        "lt-LT,lt;q=0.9,en;q=0.8"),
    "LV": ("lv-LV", "Europe/Riga",           "lv-LV,lv;q=0.9,en;q=0.8"),
    "EE": ("et-EE", "Europe/Tallinn",        "et-EE,et;q=0.9,en;q=0.8"),
    "BY": ("be-BY", "Europe/Minsk",          "be-BY,be;q=0.9,ru;q=0.8,en;q=0.7"),
    "MD": ("ro-MD", "Europe/Chisinau",       "ro-MD,ro;q=0.9,ru;q=0.8,en;q=0.7"),
    "MK": ("mk-MK", "Europe/Skopje",         "mk-MK,mk;q=0.9,en;q=0.8"),
    "AL": ("sq-AL", "Europe/Tirane",         "sq-AL,sq;q=0.9,en;q=0.8"),
    "IE": ("en-IE", "Europe/Dublin",         "en-IE,en;q=0.9"),
    "IS": ("is-IS", "Atlantic/Reykjavik",    "is-IS,is;q=0.9,en;q=0.8"),
    # Americas
    "CL": ("es-CL", "America/Santiago",      "es-CL,es;q=0.9,en;q=0.8"),
    "CO": ("es-CO", "America/Bogota",        "es-CO,es;q=0.9,en;q=0.8"),
    "PE": ("es-PE", "America/Lima",          "es-PE,es;q=0.9,en;q=0.8"),
    "VE": ("es-VE", "America/Caracas",       "es-VE,es;q=0.9,en;q=0.8"),
    "EC": ("es-EC", "America/Guayaquil",     "es-EC,es;q=0.9,en;q=0.8"),
    "BO": ("es-BO", "America/La_Paz",        "es-BO,es;q=0.9,en;q=0.8"),
    "PY": ("es-PY", "America/Asuncion",      "es-PY,es;q=0.9,en;q=0.8"),
    "UY": ("es-UY", "America/Montevideo",    "es-UY,es;q=0.9,en;q=0.8"),
    "CR": ("es-CR", "America/Costa_Rica",    "es-CR,es;q=0.9,en;q=0.8"),
    "GT": ("es-GT", "America/Guatemala",     "es-GT,es;q=0.9,en;q=0.8"),
    "CU": ("es-CU", "America/Havana",        "es-CU,es;q=0.9,en;q=0.8"),
    "DO": ("es-DO", "America/Santo_Domingo", "es-DO,es;q=0.9,en;q=0.8"),
    "NZ": ("en-NZ", "Pacific/Auckland",      "en-NZ,en;q=0.9"),
    # Oceania / Pacific
    "PG": ("en-PG", "Pacific/Port_Moresby",  "en-PG,en;q=0.9"),
    "FJ": ("en-FJ", "Pacific/Fiji",          "en-FJ,en;q=0.9"),
}

_parser = argparse.ArgumentParser(add_help=False)
_parser.add_argument("-T", "--tor",        action="store_true")
_parser.add_argument("-P", "--proxy",      action="store_true")
_parser.add_argument("--debug",            action="store_true")
_parser.add_argument("--socks-port",       type=int, default=int(os.environ.get("SOCKS_PORT", 9050)))
_parser.add_argument("--control-port",     type=int, default=int(os.environ.get("CONTROL_PORT", 9051)))
_parser.add_argument("--api-port",         type=int, default=int(os.environ.get("API_PORT", 5000)))
_args, _ = _parser.parse_known_args()

TOR_PROXY    = f"socks5://127.0.0.1:{_args.socks_port}"
CONTROL_PORT = _args.control_port
API_BASE     = f"http://127.0.0.1:{_args.api_port}"

def tor_reset_and_get_ip() -> str:
    # wait for bootstrap via API
    for _ in range(24):  # 2 min max
        try:
            r = requests.get(f"{API_BASE}/status", timeout=5)
            if r.json().get("bootstrapped"):
                break
        except Exception:
            pass
        print(f"[~] Waiting for Tor API on {API_BASE}...")
        time.sleep(5)
    # reset IP via API
    try:
        requests.get(f"{API_BASE}/reset-ip", timeout=5)
    except Exception as e:
        print(f"[-] reset-ip failed: {e}")
    time.sleep(5)
    return get_ip_info(TOR_PROXY).get("ip", "unknown")

def log_ip(ip: str):
    with open(LOG_FILE, "a") as f:
        f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} | {ip}\n")

def generate_identity():
    name   = ''.join(random.choices(string.ascii_lowercase, k=8))
    number = random.randint(1000, 9999)
    email  = f"kalawssimatrix+{number}@gmail.com"
    print("=" * 40)
    print(f"  Name : {name}")
    print(f"  Email: {email}")
    print("=" * 40)
    return name, email

VIEWPORTS = [
    {"width": 1920, "height": 1080},
    {"width": 1366, "height": 768},
    {"width": 1536, "height": 864},
    {"width": 1440, "height": 900},
    {"width": 1280, "height": 720},
]

def run_session(elements: dict, session_id: int = 0, proxy_config: dict = None):
    tag = f"[{session_id % 100:02d}]"
    _print = lambda msg: print(f"{tag} {msg}")
    if _args.tor:
        tor_ip = tor_reset_and_get_ip()
        log_ip(tor_ip)
        proxy_config = proxy_config or {"server": TOR_PROXY}
    else:
        tor_ip = "N/A"
        proxy_config = proxy_config or {}

    proxy_url = proxy_config.get("server") if proxy_config else None
    ip_info   = get_ip_info(proxy_url)
    cc        = (ip_info.get("country_code") or "US").upper()
    locale, chosen_tz, accept_lang = CC_LANG.get(cc, CC_LANG["US"])
    # US has multiple timezones — randomize to avoid always mapping to New York
    if cc == "US":
        chosen_tz = random.choice([
            "America/New_York", "America/Chicago", "America/Denver",
            "America/Los_Angeles", "America/Phoenix", "America/Anchorage",
        ])
    lang_primary = locale
    # correct lang_base for zh variants (zh-CN→zh-Hans, zh-HK/TW→zh-Hant)
    if locale.startswith("zh-"):
        lang_base = "zh-Hans" if locale == "zh-CN" else "zh-Hant"
    else:
        lang_base = locale.split("-")[0]
    _print(f"IP: {ip_info.get('ip')} | {ip_info.get('country')} ({cc}) → locale={locale} tz={chosen_tz}")
    os.environ["TZ"] = chosen_tz
    time.tzset()

    session_profile = os.path.join(BASE_DIR, f"playwright-profile-{session_id}")
    # clean profile before AND register cleanup on crash
    import shutil
    def _cleanup():
        shutil.rmtree(session_profile, ignore_errors=True)
    atexit.register(_cleanup)
    _cleanup()
    os.makedirs(session_profile, exist_ok=True)

    EMAIL = elements.get("email")
    NAME  = elements.get("name", "N/A")

    # Playwright version → Chromium version mapping
    PLAYWRIGHT_CHROMIUM_VERSION = {
        "1.44": "124", "1.43": "123", "1.42": "122", "1.41": "121",
        "1.40": "120", "1.39": "119",
    }
    import importlib.metadata
    _pw_ver = ".".join(importlib.metadata.version("playwright").split(".")[:2])
    _chrome_ver = PLAYWRIGHT_CHROMIUM_VERSION.get(_pw_ver, "124")

    # Desktop UAs from user_agnt + version-matched Chrome fallbacks
    DESKTOP_UAS = list(user_agnt.user_agent_list) or [
        f"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{_chrome_ver}.0.0.0 Safari/537.36",
        f"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{_chrome_ver}.0.0.0 Safari/537.36 Edg/{_chrome_ver}.0.0.0",
        f"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{_chrome_ver}.0.0.0 Safari/537.36",
        f"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{_chrome_ver}.0.0.0 Safari/537.36",
    ]

    # Mobile device profiles: (ua, viewport, touch, webgl_vendor, webgl_renderer, hw, mem, platform, ua_platform, ua_platform_ver, model)
    MOBILE_PROFILES = [
        (f"Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{_chrome_ver}.0.0.0 Mobile Safari/537.36",
         {"width": 393, "height": 851},  True, "Google Inc.", "ANGLE (Qualcomm, Adreno (TM) 730, OpenGL ES 3.2)", 8, 8, "Linux armv8l", "Android", "13.0.0", "Pixel 7"),
        (f"Mozilla/5.0 (Linux; Android 13; Pixel 6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{_chrome_ver}.0.0.0 Mobile Safari/537.36",
         {"width": 412, "height": 915},  True, "Google Inc.", "ANGLE (Qualcomm, Adreno (TM) 650, OpenGL ES 3.2)", 8, 8, "Linux armv8l", "Android", "13.0.0", "Pixel 6"),
        (f"Mozilla/5.0 (Linux; Android 12; SM-G991B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{_chrome_ver}.0.0.0 Mobile Safari/537.36",
         {"width": 360, "height": 800},  True, "Google Inc.", "ANGLE (ARM, Mali-G78, OpenGL ES 3.2)", 8, 8, "Linux armv8l", "Android", "12.0.0", "SM-G991B"),
        (f"Mozilla/5.0 (Linux; Android 13; SM-S918B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{_chrome_ver}.0.0.0 Mobile Safari/537.36",
         {"width": 360, "height": 780},  True, "Google Inc.", "ANGLE (Qualcomm, Adreno (TM) 740, OpenGL ES 3.2)", 8, 12, "Linux armv8l", "Android", "13.0.0", "SM-S918B"),
        (f"Mozilla/5.0 (Linux; Android 12; Redmi Note 11) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{_chrome_ver}.0.0.0 Mobile Safari/537.36",
         {"width": 393, "height": 873},  True, "Google Inc.", "ANGLE (Qualcomm, Adreno (TM) 680, OpenGL ES 3.2)", 8, 6, "Linux armv8l", "Android", "12.0.0", "Redmi Note 11"),
        ("Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
         {"width": 390, "height": 844},  True, "Apple Inc.", "Apple A16 GPU", 6, 6, "iPhone", "iOS", "17.0.0", "iPhone"),
        ("Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Mobile/15E148 Safari/604.1",
         {"width": 375, "height": 812},  True, "Apple Inc.", "Apple A15 GPU", 6, 6, "iPhone", "iOS", "16.6.0", "iPhone"),
        ("Mozilla/5.0 (iPad; CPU OS 16_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Mobile/15E148 Safari/604.1",
         {"width": 820, "height": 1180}, True, "Apple Inc.", "Apple M1 GPU", 8, 8, "iPad",   "iOS", "16.6.0", "iPad"),
    ]

    # 70% desktop, 30% mobile — mirrors real traffic split
    is_mobile = random.random() < 0.30

    with sync_playwright() as p:
        browser_type = p.chromium

        if is_mobile:
            mob = random.choice(MOBILE_PROFILES)
            (chrome_ua, chosen_viewport, _has_touch,
             _webgl_v, _webgl_r, _hw, _mem,
             _platform, _ua_platform, _ua_platform_ver, _model) = mob
            is_ios = "iPhone" in chrome_ua or "iPad" in chrome_ua
        else:
            chosen_viewport = random.choice(VIEWPORTS)
            chrome_ua       = random.choice(DESKTOP_UAS)
            is_ios          = False

        _print("\n" + "═" * 52)
        _print(f"  SESSION #{session_id}")
        _print("═" * 52)
        _print(f"  {'Version':<14}: {VERSION}")
        _print(f"  {'Device':<14}: {'Mobile' if is_mobile else 'Desktop'}")
        _print(f"  {'Name':<14}: {NAME}")
        _print(f"  {'Email':<14}: {EMAIL}")
        _print(f"  {'Proxy':<14}: {proxy_url or TOR_PROXY}")
        _print(f"  {'Tor IP':<14}: {tor_ip}")
        _print(f"  {'User-Agent':<14}: {chrome_ua[:55]}...")
        _print(f"  {'Viewport':<14}: {chosen_viewport['width']}x{chosen_viewport['height']}")
        _print(f"  {'Timezone':<14}: {chosen_tz}")
        _print(f"  {'Debug':<14}: {_args.debug}")
        _print("═" * 52 + "\n")

        launch_kwargs = dict(
            headless=not _args.debug,
            proxy=proxy_config,
            user_agent=chrome_ua,
            viewport=chosen_viewport,
            locale=locale,
            timezone_id=chosen_tz,
            has_touch=is_mobile,
            is_mobile=is_mobile,
            extra_http_headers={"Accept-Language": accept_lang},
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-infobars",
                "--disable-dev-shm-usage",
                f"--window-size={chosen_viewport['width']},{chosen_viewport['height']}",
            ],
            ignore_default_args=["--enable-automation"],
        )
        if os.path.exists("/usr/bin/google-chrome"):
            launch_kwargs["channel"] = "chrome"

        context = browser_type.launch_persistent_context(session_profile, **launch_kwargs)
        try:
            page = context.pages[0] if context.pages else context.new_page()

            hw_concurrency  = _hw  if is_mobile else random.choice([2, 4, 8])
            device_memory   = _mem if is_mobile else random.choice([2, 4, 8])
            battery_level   = round(random.uniform(0.6, 1.0), 2)
            battery_charging = random.choice(["true", "false"])
            rtt             = random.choice([50, 100, 150])
            downlink        = random.choice([5, 10, 20])
            canvas_salt     = random.randint(1, 255)
            audio_salt      = round(random.uniform(0.001, 0.009), 4)
            tz_offset_map   = {
                "America/New_York": 300, "America/Sao_Paulo": 180, "America/Mexico_City": 360,
                "Europe/London": 0, "Europe/Rome": -60, "Europe/Berlin": -60,
                "Europe/Paris": -60, "Europe/Madrid": -60, "Europe/Amsterdam": -60,
                "Europe/Warsaw": -60, "Europe/Moscow": -180, "Europe/Istanbul": -180,
                "Europe/Stockholm": -60, "Asia/Tokyo": -540, "Asia/Shanghai": -480,
            }
            tz_offset = tz_offset_map.get(chosen_tz, 0)
            # WebGL vendor/renderer must match the UA's OS — Chrome always uses ANGLE
            _webgl_vendors_windows = [
                ("Google Inc.", "ANGLE (Intel, Intel(R) UHD Graphics 630 Direct3D11 vs_5_0 ps_5_0, D3D11)"),
                ("Google Inc.", "ANGLE (Intel, Intel(R) UHD Graphics 620 Direct3D11 vs_5_0 ps_5_0, D3D11)"),
                ("Google Inc.", "ANGLE (Intel, Intel(R) Iris(R) Xe Graphics Direct3D11 vs_5_0 ps_5_0, D3D11)"),
                ("Google Inc.", "ANGLE (Intel, Intel(R) Iris(R) Plus Graphics Direct3D11 vs_5_0 ps_5_0, D3D11)"),
                ("Google Inc.", "ANGLE (NVIDIA, NVIDIA GeForce GTX 1650 Direct3D11 vs_5_0 ps_5_0, D3D11)"),
                ("Google Inc.", "ANGLE (NVIDIA, NVIDIA GeForce GTX 1660 Ti Direct3D11 vs_5_0 ps_5_0, D3D11)"),
                ("Google Inc.", "ANGLE (NVIDIA, NVIDIA GeForce RTX 2070 Direct3D11 vs_5_0 ps_5_0, D3D11)"),
                ("Google Inc.", "ANGLE (NVIDIA, NVIDIA GeForce RTX 3060 Direct3D11 vs_5_0 ps_5_0, D3D11)"),
                ("Google Inc.", "ANGLE (NVIDIA, NVIDIA GeForce RTX 3070 Direct3D11 vs_5_0 ps_5_0, D3D11)"),
                ("Google Inc.", "ANGLE (NVIDIA, NVIDIA GeForce RTX 3080 Direct3D11 vs_5_0 ps_5_0, D3D11)"),
                ("Google Inc.", "ANGLE (NVIDIA, NVIDIA GeForce RTX 4070 Direct3D11 vs_5_0 ps_5_0, D3D11)"),
                ("Google Inc.", "ANGLE (AMD, AMD Radeon RX 570 Direct3D11 vs_5_0 ps_5_0, D3D11)"),
                ("Google Inc.", "ANGLE (AMD, AMD Radeon RX 580 Direct3D11 vs_5_0 ps_5_0, D3D11)"),
                ("Google Inc.", "ANGLE (AMD, AMD Radeon RX 6600 XT Direct3D11 vs_5_0 ps_5_0, D3D11)"),
                ("Google Inc.", "ANGLE (AMD, AMD Radeon RX 6700 XT Direct3D11 vs_5_0 ps_5_0, D3D11)"),
                ("Google Inc.", "ANGLE (AMD, AMD Radeon(TM) Graphics Direct3D11 vs_5_0 ps_5_0, D3D11)"),
            ]
            _webgl_vendors_mac = [
                ("Google Inc.", "ANGLE (Apple, ANGLE Metal Renderer: Apple M1, Unspecified Version)"),
                ("Google Inc.", "ANGLE (Apple, ANGLE Metal Renderer: Apple M1 Pro, Unspecified Version)"),
                ("Google Inc.", "ANGLE (Apple, ANGLE Metal Renderer: Apple M2, Unspecified Version)"),
                ("Google Inc.", "ANGLE (Apple, ANGLE Metal Renderer: Apple M2 Pro, Unspecified Version)"),
                ("Google Inc.", "ANGLE (Apple, ANGLE Metal Renderer: Apple M3, Unspecified Version)"),
                ("Google Inc.", "ANGLE (Intel, ANGLE Metal Renderer: Intel(R) Iris(R) Plus Graphics, Unspecified Version)"),
                ("Google Inc.", "ANGLE (AMD, ANGLE Metal Renderer: AMD Radeon Pro 5500M, Unspecified Version)"),
            ]
            _webgl_vendors_linux = [
                ("Google Inc.", "ANGLE (Intel, Mesa Intel(R) UHD Graphics 620 (KBL GT2), OpenGL 4.6)"),
                ("Google Inc.", "ANGLE (Intel, Mesa Intel(R) UHD Graphics 630 (CFL GT2), OpenGL 4.6)"),
                ("Google Inc.", "ANGLE (Intel, Mesa Intel(R) Iris(R) Xe Graphics (TGL GT2), OpenGL 4.6)"),
                ("Google Inc.", "ANGLE (AMD, AMD Radeon RX 580 (polaris10, LLVM 15.0.7, DRM 3.49, 6.1.0), OpenGL 4.6)"),
                ("Google Inc.", "ANGLE (NVIDIA, NVIDIA GeForce GTX 1060/PCIe/SSE2, OpenGL 4.6)"),
                ("Google Inc.", "ANGLE (NVIDIA, NVIDIA GeForce RTX 3060/PCIe/SSE2, OpenGL 4.6)"),
            ]

            if is_mobile:
                webgl_vendor, webgl_renderer = _webgl_v, _webgl_r
            elif "Windows" in chrome_ua:
                webgl_vendor, webgl_renderer = random.choice(_webgl_vendors_windows)
            elif "Macintosh" in chrome_ua:
                webgl_vendor, webgl_renderer = random.choice(_webgl_vendors_mac)
            else:
                webgl_vendor, webgl_renderer = random.choice(_webgl_vendors_linux)

            # Derive platform string from UA for consistency
            if is_mobile:
                _platform        = _platform
                _ua_platform     = _ua_platform
                _ua_platform_ver = _ua_platform_ver
            elif "Windows" in chrome_ua:
                _platform = "Win32"
                _ua_platform = "Windows"
                _ua_platform_ver = "10.0.0"
            elif "Macintosh" in chrome_ua:
                _platform = "MacIntel"
                _ua_platform = "macOS"
                _ua_platform_ver = "13.0.0"
            else:
                _platform = "Linux x86_64"
                _ua_platform = "Linux"
                _ua_platform_ver = "5.15.0"

            toolbar_height = random.randint(72, 92)
            dpr = random.choice([1, 1.25, 1.5, 2]) if not is_mobile else random.choice([2, 2.5, 3])
            # appVersion = everything after "Mozilla/" in the UA
            _app_version = chrome_ua.replace("Mozilla/", "", 1) if chrome_ua.startswith("Mozilla/") else chrome_ua

            context.add_init_script(f"""
                // ── 0. Native toString hardening — must run first ──
                (function() {{
                    const _nativeToString = Function.prototype.toString;
                    const _registry = new WeakMap();
                    Function.prototype.toString = function() {{
                        return _registry.has(this) ? _registry.get(this) : _nativeToString.call(this);
                    }};
                    // expose helper for patched functions to register their native string
                    window.__nativeReg = (fn, name) => {{
                        _registry.set(fn, `function ${{name}}() {{ [native code] }}`);
                        return fn;
                    }};
                }})();

                // ── 1. webdriver — delete from prototype so descriptor check passes ──
                try {{ delete Object.getPrototypeOf(navigator).webdriver; }} catch(e) {{}}

                // ── 2. window.chrome ──
                window.chrome = {{
                    app: {{ isInstalled: false, InstallState: {{}}, RunningState: {{}} }},
                    csi: () => ({{ pageT: Date.now(), startE: Date.now(), tran: Math.floor(Math.random()*20)+1 }}),
                    loadTimes: () => {{
                        const t = Date.now() / 1000;
                        return {{
                            commitLoadTime: t - 0.4, connectionInfo: 'h2',
                            finishDocumentLoadTime: t - 0.1, finishLoadTime: t,
                            firstPaintAfterLoadTime: 0, firstPaintTime: t - 0.3,
                            navigationType: 'Other', npnNegotiatedProtocol: 'h2',
                            requestTime: t - 0.5, startLoadTime: t - 0.5,
                            wasAlternateProtocolAvailable: false,
                            wasFetchedViaSpdy: true, wasNpnNegotiated: true,
                        }};
                    }},
                    runtime: {{
                        id: undefined, lastError: undefined,
                        OnInstalledReason: {{}}, OnRestartRequiredReason: {{}},
                        PlatformArch: {{}}, PlatformNaclArch: {{}},
                        PlatformOs: {{}}, RequestUpdateCheckStatus: {{}},
                        connect: () => undefined, sendMessage: () => undefined,
                    }},
                    webstore: {{ onInstallStageChanged: {{}}, onDownloadProgress: {{}} }},
                }};

                // ── 3. plugins — 3 entries matching real Chrome ──
                (function() {{
                    function _mime(type, suf, desc) {{
                        return {{ type, suffixes: suf, description: desc, enabledPlugin: null }};
                    }}
                    function _plugin(name, fn, desc, mimes) {{
                        const p = {{ name, filename: fn, description: desc, length: mimes.length }};
                        mimes.forEach((m, i) => {{ p[i] = m; m.enabledPlugin = p; }});
                        p.item = (i) => p[i] ?? null;
                        p.namedItem = (n) => mimes.find(m => m.type === n) ?? null;
                        p[Symbol.iterator] = function*() {{ for (let i=0;i<this.length;i++) yield this[i]; }};
                        return p;
                    }}
                    const pdf1 = _mime('application/pdf', 'pdf', 'Portable Document Format');
                    const pdf2 = _mime('text/pdf', 'pdf', 'Portable Document Format');
                    const plugins = [
                        _plugin('PDF Viewer',         'internal-pdf-viewer', 'Portable Document Format', [pdf1, pdf2]),
                        _plugin('Chrome PDF Viewer',  'internal-pdf-viewer', 'Portable Document Format', [pdf1, pdf2]),
                        _plugin('Chromium PDF Viewer','internal-pdf-viewer', 'Portable Document Format', [pdf1, pdf2]),
                    ];
                    const pa = {{ length: plugins.length }};
                    plugins.forEach((p, i) => {{ pa[i] = p; }});
                    pa.item = (i) => pa[i] ?? null;
                    pa.namedItem = (n) => plugins.find(p => p.name === n) ?? null;
                    pa.refresh = () => {{}};
                    pa[Symbol.iterator] = function*() {{ for(let i=0;i<this.length;i++) yield this[i]; }};
                    Object.defineProperty(navigator, 'plugins', {{ get: () => pa }});
                    const allMimes = [pdf1, pdf2];
                    const ma = {{ length: allMimes.length }};
                    allMimes.forEach((m, i) => {{ ma[i] = m; }});
                    ma.item = (i) => ma[i] ?? null;
                    ma.namedItem = (n) => allMimes.find(m => m.type === n) ?? null;
                    ma[Symbol.iterator] = function*() {{ for(let i=0;i<this.length;i++) yield this[i]; }};
                    Object.defineProperty(navigator, 'mimeTypes', {{ get: () => ma }});
                }})();

                // ── 4. platform ──
                Object.defineProperty(navigator, 'platform', {{ get: () => '{_platform}' }});

                // ── 5. userAgentData — cached object, correct brand order ──
                (function() {{
                    const _brands = [
                        {{ brand: 'Not=A?Brand',   version: '99'           }},
                        {{ brand: 'Chromium',      version: '{_chrome_ver}' }},
                        {{ brand: 'Google Chrome', version: '{_chrome_ver}' }},
                    ];
                    const _uad = {{
                        brands: _brands,
                        mobile: {'true' if is_mobile else 'false'},
                        platform: '{_ua_platform}',
                        toJSON: () => ({{ brands: _brands, mobile: {'true' if is_mobile else 'false'}, platform: '{_ua_platform}' }}),
                        getHighEntropyValues: (hints) => Promise.resolve({{
                            architecture: {'""' if is_mobile else '"x86"'},
                            bitness: {'""' if is_mobile else '"64"'},
                            brands: _brands,
                            fullVersionList: [
                                {{ brand: 'Not=A?Brand',   version: '99.0.0.0'             }},
                                {{ brand: 'Chromium',      version: '{_chrome_ver}.0.0.0'  }},
                                {{ brand: 'Google Chrome', version: '{_chrome_ver}.0.0.0'  }},
                            ],
                            mobile: {'true' if is_mobile else 'false'},
                            model: '{"" if not is_mobile else _model}',
                            platform: '{_ua_platform}',
                            platformVersion: '{_ua_platform_ver}',
                            uaFullVersion: '{_chrome_ver}.0.0.0',
                            wow64: false,
                        }}),
                    }};
                    Object.defineProperty(navigator, 'userAgentData', {{ get: () => _uad }});
                }})();

                // ── 6. touch ──
                {'Object.defineProperty(navigator, "maxTouchPoints", { get: () => 5 });' if is_mobile else 'Object.defineProperty(navigator, "maxTouchPoints", { get: () => 0 });'}
                {'window.ontouchstart = null;' if is_mobile else ''}

                // ── 7. language / locale ──
                Object.defineProperty(navigator, 'languages', {{ get: () => {json.dumps([t.split(';')[0] for t in accept_lang.split(',')])} }});
                Object.defineProperty(navigator, 'language',  {{ get: () => '{lang_primary}' }});

                // ── 8. misc navigator ──
                Object.defineProperty(navigator, 'vendor',        {{ get: () => 'Google Inc.' }});
                Object.defineProperty(navigator, 'doNotTrack',    {{ get: () => null }});
                Object.defineProperty(navigator, 'cookieEnabled', {{ get: () => true }});
                Object.defineProperty(navigator, 'appName',       {{ get: () => 'Netscape' }});
                Object.defineProperty(navigator, 'appVersion',    {{ get: () => '{_app_version}' }});
                Object.defineProperty(navigator, 'product',       {{ get: () => 'Gecko' }});
                Object.defineProperty(navigator, 'productSub',    {{ get: () => '20030107' }});

                // ── 9. window geometry ──
                const _toolbarH = {toolbar_height};
                Object.defineProperty(window,  'outerHeight', {{ get: () => window.innerHeight + _toolbarH }});
                Object.defineProperty(window,  'outerWidth',  {{ get: () => window.innerWidth }});
                Object.defineProperty(screen,  'width',       {{ get: () => {chosen_viewport['width']}  }});
                Object.defineProperty(screen,  'height',      {{ get: () => {chosen_viewport['height']} }});
                Object.defineProperty(screen,  'availWidth',  {{ get: () => {chosen_viewport['width']}  }});
                Object.defineProperty(screen,  'availHeight', {{ get: () => {chosen_viewport['height']} }});
                Object.defineProperty(screen,  'availTop',    {{ get: () => 0 }});
                Object.defineProperty(screen,  'availLeft',   {{ get: () => 0 }});
                Object.defineProperty(screen,  'colorDepth',  {{ get: () => 24 }});
                Object.defineProperty(screen,  'pixelDepth',  {{ get: () => 24 }});
                Object.defineProperty(screen,  'orientation', {{ get: () => ({{
                    type: '{chosen_viewport['width'] > chosen_viewport['height'] and 'landscape-primary' or 'portrait-primary'}',
                    angle: 0, addEventListener: () => {{}}, removeEventListener: () => {{}}
                }}) }});
                Object.defineProperty(window, 'devicePixelRatio', {{ get: () => {dpr} }});

                // ── 10. hardware ──
                Object.defineProperty(navigator, 'hardwareConcurrency', {{ get: () => {hw_concurrency} }});
                Object.defineProperty(navigator, 'deviceMemory',        {{ get: () => {device_memory}  }});

                // ── 11. canvas noise — single _orig reference, no double-noise ──
                (function() {{
                    const SALT = {canvas_salt};
                    const _orig2d = CanvasRenderingContext2D.prototype.getImageData;

                    function _noise(data) {{
                        for (let i = 0; i < data.length; i += 4) {{
                            data[i]     ^= SALT;
                            data[i + 1] ^= (SALT * 3) & 0xff;
                            data[i + 2] ^= (SALT * 7) & 0xff;
                        }}
                    }}

                    CanvasRenderingContext2D.prototype.getImageData = __nativeReg(function(...a) {{
                        const d = _orig2d.apply(this, a);
                        _noise(d.data);
                        return d;
                    }}, 'getImageData');

                    const _origURL  = HTMLCanvasElement.prototype.toDataURL;
                    const _origBlob = HTMLCanvasElement.prototype.toBlob;

                    function _applyNoise(canvas) {{
                        const ctx = canvas.getContext('2d');
                        if (ctx && canvas.width && canvas.height) {{
                            const img = _orig2d.call(ctx, 0, 0, canvas.width, canvas.height);
                            _noise(img.data);
                            ctx.putImageData(img, 0, 0);
                        }}
                    }}

                    HTMLCanvasElement.prototype.toDataURL = __nativeReg(function(...a) {{
                        _applyNoise(this);
                        return _origURL.apply(this, a);
                    }}, 'toDataURL');

                    HTMLCanvasElement.prototype.toBlob = __nativeReg(function(cb, ...a) {{
                        _applyNoise(this);
                        return _origBlob.call(this, cb, ...a);
                    }}, 'toBlob');

                    if (typeof OffscreenCanvasRenderingContext2D !== 'undefined') {{
                        const _origOff = OffscreenCanvasRenderingContext2D.prototype.getImageData;
                        OffscreenCanvasRenderingContext2D.prototype.getImageData = __nativeReg(function(...a) {{
                            const d = _origOff.apply(this, a);
                            _noise(d.data);
                            return d;
                        }}, 'getImageData');
                    }}
                }})();

                // ── 12. WebGL — vendor/renderer + extra params + readPixels noise ──
                (function() {{
                    const V = '{webgl_vendor}', R = '{webgl_renderer}', S = {canvas_salt};
                    function _patch(proto) {{
                        const _gp = proto.getParameter;
                        proto.getParameter = __nativeReg(function(p) {{
                            if (p === 37445) return V;   // UNMASKED_VENDOR_WEBGL
                            if (p === 37446) return R;   // UNMASKED_RENDERER_WEBGL
                            if (p === 7937)  return R;   // RENDERER (same as unmasked)
                            if (p === 7936)  return V;   // VENDOR
                            if (p === 3379)  return 16384;  // MAX_TEXTURE_SIZE
                            if (p === 34076) return 16384;  // MAX_CUBE_MAP_TEXTURE_SIZE
                            if (p === 3386)  return new Int32Array([16384, 16384]); // MAX_VIEWPORT_DIMS
                            if (p === 33902) return new Float32Array([1, 1]);       // ALIASED_LINE_WIDTH_RANGE
                            if (p === 33901) return new Float32Array([1, 1024]);    // ALIASED_POINT_SIZE_RANGE
                            return _gp.call(this, p);
                        }}, 'getParameter');
                        const _rp = proto.readPixels;
                        proto.readPixels = __nativeReg(function(...a) {{
                            _rp.apply(this, a);
                            const buf = a[6];
                            if (buf instanceof Uint8Array && buf.length >= 4) {{
                                // noise full first pixel (RGBA)
                                buf[0] ^= S;
                                buf[1] ^= (S * 3) & 0xff;
                                buf[2] ^= (S * 7) & 0xff;
                                buf[3] ^= (S * 5) & 0xff;
                            }}
                        }}, 'readPixels');
                        const _gse = proto.getSupportedExtensions;
                        proto.getSupportedExtensions = __nativeReg(function() {{
                            const exts = _gse.call(this) || [];
                            return exts.filter(e => !e.includes('debug') && !e.includes('WEBGL_debug'));
                        }}, 'getSupportedExtensions');
                    }}
                    _patch(WebGLRenderingContext.prototype);
                    _patch(WebGL2RenderingContext.prototype);
                }})();

                // ── 13. AudioContext — noise OfflineAudioContext (actual fingerprint vector) ──
                (function() {{
                    const ASALT = {audio_salt};
                    const _origStart = OfflineAudioContext.prototype.startRendering;
                    OfflineAudioContext.prototype.startRendering = __nativeReg(function() {{
                        return _origStart.call(this).then(buf => {{
                            const ch = buf.getChannelData(0);
                            for (let i = 0; i < ch.length; i++) ch[i] += (Math.random() - 0.5) * ASALT;
                            return buf;
                        }});
                    }}, 'startRendering');
                    const _origGCD = AudioBuffer.prototype.getChannelData;
                    AudioBuffer.prototype.getChannelData = __nativeReg(function(ch) {{
                        const data = _origGCD.call(this, ch);
                        if (ch === 0) data[0] += ASALT * 0.0001;
                        return data;
                    }}, 'getChannelData');
                }})();

                // ── 14. Battery ──
                navigator.getBattery = () => Promise.resolve({{
                    charging: {battery_charging}, level: {battery_level},
                    chargingTime: 0, dischargingTime: Infinity,
                    addEventListener: () => {{}}
                }});

                // ── 15. Network — cached, with all fields ──
                (function() {{
                    const _conn = {{
                        type: 'wifi', effectiveType: '4g',
                        rtt: {rtt}, downlink: {downlink}, downlinkMax: Infinity,
                        saveData: false, onchange: null,
                        addEventListener: () => {{}}, removeEventListener: () => {{}}
                    }};
                    Object.defineProperty(navigator, 'connection', {{ get: () => _conn }});
                }})();

                // ── 16. Permissions ──
                (function() {{
                    const _pq = navigator.permissions.query.bind(navigator.permissions);
                    navigator.permissions.query = __nativeReg(function(p) {{
                        if (p.name === 'notifications') return Promise.resolve({{ state: 'default' }});
                        if (p.name === 'geolocation')   return Promise.resolve({{ state: 'prompt'  }});
                        if (p.name === 'camera')        return Promise.resolve({{ state: 'prompt'  }});
                        if (p.name === 'microphone')    return Promise.resolve({{ state: 'prompt'  }});
                        return _pq(p);
                    }}, 'query');
                }})();

                // ── 17. speechSynthesis — locale-matched voices ──
                (function() {{
                    const _voices = [
                        {{ voiceURI: 'Google {lang_primary}', name: 'Google {lang_primary}',
                           lang: '{lang_primary}', localService: false, default: true }},
                        {{ voiceURI: 'Google {lang_base}',    name: 'Google {lang_base}',
                           lang: '{lang_base}',    localService: false, default: false }},
                    ];
                    speechSynthesis.getVoices = () => _voices;
                    window.addEventListener('voiceschanged', () => {{}}, {{ once: true }});
                }})();

                // ── 18. mediaDevices — random hex deviceIds ──
                (function() {{
                    function _rnd() {{ return crypto.randomUUID ? crypto.randomUUID() : Math.random().toString(16).slice(2).padEnd(36,'0'); }}
                    const _devs = [
                        {{ kind: 'audioinput',  label: '', deviceId: _rnd(), groupId: _rnd() }},
                        {{ kind: 'audiooutput', label: '', deviceId: _rnd(), groupId: _rnd() }},
                        {{ kind: 'videoinput',  label: '', deviceId: _rnd(), groupId: _rnd() }},
                    ];
                    if (navigator.mediaDevices)
                        navigator.mediaDevices.enumerateDevices = __nativeReg(
                            () => Promise.resolve(_devs), 'enumerateDevices'
                        );
                }})();

                // ── 19. Error.stackTraceLimit (V8 signal) ──
                Error.stackTraceLimit = 10;

                // ── 20. Notification ──
                if (typeof Notification !== 'undefined')
                    Object.defineProperty(Notification, 'permission', {{ get: () => 'default' }});

                // ── 21. performance.now() jitter — defeat timing fingerprint ──
                (function() {{
                    const _origNow = performance.now.bind(performance);
                    performance.now = __nativeReg(function() {{
                        return _origNow() + (Math.random() - 0.5) * 0.1;
                    }}, 'now');
                }})();

                // ── 22. history.length — simulate real user ──
                (function() {{
                    const _fakeLen = Math.floor(Math.random() * 8) + 2;
                    try {{ Object.defineProperty(history, 'length', {{ get: () => _fakeLen }}); }} catch(e) {{}}
                }})();

                // ── 23. iframe contentWindow isolation — patch Navigator prototype ──
                (function() {{
                    // Re-apply webdriver deletion on every new document via MutationObserver
                    // and patch iframes as they are inserted
                    const _origCreateElement = document.createElement.bind(document);
                    document.createElement = __nativeReg(function(tag, ...a) {{
                        const el = _origCreateElement(tag, ...a);
                        if (tag.toLowerCase() === 'iframe') {{
                            el.addEventListener('load', () => {{
                                try {{
                                    const w = el.contentWindow;
                                    if (w && w.navigator)
                                        delete Object.getPrototypeOf(w.navigator).webdriver;
                                }} catch(e) {{}}
                            }});
                        }}
                        return el;
                    }}, 'createElement');
                }})();

                // ── 24. window.name persistence simulation ──
                if (!window.name) window.name = '';

                // ── 25. Intl consistency — timeZone + locale must match context ──
                (function() {{
                    const _origDTF = Intl.DateTimeFormat;
                    Intl.DateTimeFormat = __nativeReg(function(loc, opts) {{
                        opts = opts || {{}};
                        if (!opts.timeZone) opts.timeZone = '{chosen_tz}';
                        return new _origDTF(loc || '{lang_primary}', opts);
                    }}, 'DateTimeFormat');
                    Intl.DateTimeFormat.prototype = _origDTF.prototype;
                    Intl.DateTimeFormat.supportedLocalesOf = _origDTF.supportedLocalesOf.bind(_origDTF);
                }})();

                // ── 26. RTCPeerConnection — block local IP leak ──
                (function() {{
                    const _origRTC = window.RTCPeerConnection;
                    if (!_origRTC) return;
                    window.RTCPeerConnection = __nativeReg(function(cfg, ...a) {{
                        cfg = cfg || {{}};
                        // force TURN-only to prevent local candidate gathering
                        cfg.iceTransportPolicy = 'relay';
                        const pc = new _origRTC(cfg, ...a);
                        const _origGLC = pc.createOffer.bind(pc);
                        return pc;
                    }}, 'RTCPeerConnection');
                    window.RTCPeerConnection.prototype = _origRTC.prototype;
                }})();

                // ── 27. IntersectionObserver — realistic async callbacks ──
                (function() {{
                    const _origIO = window.IntersectionObserver;
                    window.IntersectionObserver = __nativeReg(function(cb, opts) {{
                        const _io = new _origIO((entries, obs) => {{
                            // add small random delay to mimic real layout engine
                            setTimeout(() => cb(entries, obs), Math.random() * 50 + 10);
                        }}, opts);
                        return _io;
                    }}, 'IntersectionObserver');
                    window.IntersectionObserver.prototype = _origIO.prototype;
                }})();

                // ── 28. visualViewport ──
                (function() {{
                    if (!window.visualViewport) return;
                    const _vvp = window.visualViewport;
                    Object.defineProperty(_vvp, 'scale',        {{ get: () => 1.0 }});
                    Object.defineProperty(_vvp, 'offsetLeft',   {{ get: () => 0   }});
                    Object.defineProperty(_vvp, 'offsetTop',    {{ get: () => 0   }});
                    Object.defineProperty(_vvp, 'pageLeft',     {{ get: () => 0   }});
                    Object.defineProperty(_vvp, 'pageTop',      {{ get: () => 0   }});
                    Object.defineProperty(_vvp, 'width',        {{ get: () => window.innerWidth  }});
                    Object.defineProperty(_vvp, 'height',       {{ get: () => window.innerHeight }});
                }})();

                // ── 29. CSS APIs — signal real Chrome ──
                if (typeof CSS === 'undefined') window.CSS = {{}};
                if (!CSS.supports) CSS.supports = __nativeReg(function(p, v) {{
                    return true; // conservative — real Chrome supports most things
                }}, 'supports');
                if (!CSS.escape) CSS.escape = __nativeReg(function(s) {{
                    return s.replace(/([^\w-])/g, '\\\\$1');
                }}, 'escape');

                // ── 30. Chrome-specific globals ──
                if (typeof trustedTypes === 'undefined') {{
                    window.trustedTypes = {{
                        createPolicy: (name, rules) => ({{
                            createHTML:     (s) => s,
                            createScript:   (s) => s,
                            createScriptURL:(s) => s,
                        }}),
                        isHTML: () => false, isScript: () => false,
                        isScriptURL: () => false, getAttributeType: () => null,
                        getPropertyType: () => null, defaultPolicy: null,
                        emptyHTML: '', emptyScript: '',
                    }};
                }}
                // navigator.scheduling (Chrome 94+)
                if (!navigator.scheduling) {{
                    Object.defineProperty(navigator, 'scheduling', {{
                        get: () => ({{ isInputPending: () => false }})
                    }});
                }}
                // navigator.ink (Chrome 94+)
                if (!navigator.ink) {{
                    Object.defineProperty(navigator, 'ink', {{
                        get: () => ({{ requestPresenter: () => Promise.resolve({{
                            updateInkTrailStartPoint: () => {{}}
                        }}) }})
                    }});
                }}
                // navigator.locks
                if (!navigator.locks) {{
                    Object.defineProperty(navigator, 'locks', {{
                        get: () => ({{ request: () => Promise.resolve(), query: () => Promise.resolve({{held:[],pending:[]}}) }})
                    }});
                }}
                // navigator.keyboard
                if (!navigator.keyboard) {{
                    Object.defineProperty(navigator, 'keyboard', {{
                        get: () => ({{ getLayoutMap: () => Promise.resolve(new Map()), lock: () => Promise.resolve(), unlock: () => {{}} }})
                    }});
                }}

                // ── 31. SharedArrayBuffer / crossOriginIsolated consistency ──
                try {{
                    Object.defineProperty(window, 'crossOriginIsolated', {{ get: () => false }});
                }} catch(e) {{}}

                // ── 32. Object.getOwnPropertyDescriptor hardening ──
                // Detectors call this on navigator to check if getter is native
                (function() {{
                    const _origGOPD = Object.getOwnPropertyDescriptor;
                    Object.getOwnPropertyDescriptor = __nativeReg(function(obj, prop) {{
                        const desc = _origGOPD(obj, prop);
                        if (desc && typeof desc.get === 'function') {{
                            // make the getter's toString look native
                            const _g = desc.get;
                            if (!_g.toString().includes('[native code]')) {{
                                try {{
                                    Object.defineProperty(_g, Symbol.toStringTag, {{ value: 'function' }});
                                }} catch(e) {{}}
                            }}
                        }}
                        return desc;
                    }}, 'getOwnPropertyDescriptor');
                }})();

                // ── 33. Font enumeration hardening ──
                (function() {{
                    if (!document.fonts) return;
                    const _origCheck = document.fonts.check.bind(document.fonts);
                    // common fonts that should exist on the spoofed OS
                    const _knownFonts = {json.dumps(
                        ['Arial', 'Arial Black', 'Comic Sans MS', 'Courier New', 'Georgia',
                         'Impact', 'Times New Roman', 'Trebuchet MS', 'Verdana', 'Tahoma',
                         'Segoe UI', 'Calibri', 'Cambria'] if 'Windows' in chrome_ua else
                        ['Arial', 'Helvetica', 'Times New Roman', 'Courier New', 'Georgia',
                         'Verdana', 'Trebuchet MS', 'Arial Black', 'Impact',
                         'Helvetica Neue', 'San Francisco', 'Menlo']
                    )};
                    document.fonts.check = __nativeReg(function(font, text) {{
                        const name = font.replace(/^[\\d.]+px\\s+/, '').replace(/['"]/g, '');
                        if (_knownFonts.some(f => name.includes(f))) return true;
                        return _origCheck(font, text);
                    }}, 'check');
                }})();

                // ── 34. Cleanup __nativeReg helper (don't expose it) ──
                delete window.__nativeReg;
            """)

            def _bezier_move(pg, x1, y1, x2, y2, steps=None):
                """Move mouse along a quadratic Bézier curve with random control point."""
                steps = steps or random.randint(18, 35)
                cx_ = random.uniform(min(x1, x2), max(x1, x2))
                cy_ = random.uniform(min(y1, y2) - 80, max(y1, y2) + 80)
                for i in range(1, steps + 1):
                    t = i / steps
                    mx = (1-t)**2 * x1 + 2*(1-t)*t * cx_ + t**2 * x2
                    my = (1-t)**2 * y1 + 2*(1-t)*t * cy_ + t**2 * y2
                    pg.mouse.move(mx, my)
                    time.sleep(random.uniform(0.005, 0.018))

            page.goto(f"https://mohmal.eu.org/?{EMAIL}", wait_until="domcontentloaded", timeout=60000)
            time.sleep(3)

            AD_LOG_FILE = "/logs/ads_log.jsonl"
            AD_NETWORKS = {
                "a-ads.com": "A-ADS", "acceptable.a-ads.com": "A-ADS",
                "googlesyndication.com": "Google AdSense",
                "doubleclick.net": "Google DFP",
                "adnxs.com": "AppNexus",
                "moatads.com": "Moat",
                "amazon-adsystem.com": "Amazon Ads",
                "media.net": "Media.net",
            }

            def detect_network(url: str) -> str:
                for domain, name in AD_NETWORKS.items():
                    if domain in url:
                        return name
                return None  # None = not an ad network

            # ── 1. DETECT — only iframes whose src belongs to a known ad network ──
            iframe_handles = page.locator("iframe").all()
            detected_ads = []
            for ih in iframe_handles:
                try:
                    src     = ih.get_attribute("src") or ""
                    data_aa = ih.get_attribute("data-aa") or ""
                    network = detect_network(src)
                    # include if known network OR has data-aa (A-ADS marker)
                    if not network and not data_aa:
                        continue
                    network = network or "A-ADS"
                    bb = ih.bounding_box()
                    detected_ads.append({
                        "data_aa": data_aa,
                        "src":     src,
                        "network": network,
                        "size":    f"{int(bb['width'])}x{int(bb['height'])}" if bb else "unknown",
                        "x": bb["x"] if bb else 0,
                        "y": bb["y"] if bb else 0,
                        "text": None, "click_url": None, "loaded": False,
                    })
                except Exception:
                    continue

            _print(f"\n── 1. DETECTED {len(detected_ads)} ad iframe(s) ─────────────")
            for ad in detected_ads:
                _print(f"   [{ad['network']}] id={ad['data_aa'] or 'n/a'} size={ad['size']}")

            # ── 2+3. VIEW + ANALYSE — one pass: scroll → hover → dwell → read ──
            _print("\n── 2+3. VIEW + ANALYSE ───────────────────────────────")
            for ad in detected_ads:
                try:
                    selector = f"iframe[data-aa='{ad['data_aa']}']" if ad["data_aa"] \
                               else f"iframe[src*='{ad['src'].lstrip('/').split('?')[0][:40]}']"
                    el = page.locator(selector).first
                    el.scroll_into_view_if_needed(timeout=5000)
                    time.sleep(random.uniform(0.5, 1.2))
                    bb = el.bounding_box()
                    if bb:
                        cx = bb["x"] + bb["width"]  * random.uniform(0.3, 0.7)
                        cy = bb["y"] + bb["height"] * random.uniform(0.3, 0.7)
                        _bezier_move(page, cx - random.randint(50,150), cy - random.randint(20,80), cx, cy)
                        dwell = random.uniform(3.0, 7.0)
                        _print(f"   👁  [{ad['network']}] #{ad['data_aa'] or 'n/a'} — dwell {dwell:.1f}s")
                        time.sleep(dwell)
                except Exception as e:
                    _print(f"   ⚠  Scroll/hover failed: {e}")

                # match frame — resolve // prefix and strip query string for matching
                src_key = ad["src"].lstrip("/").split("?")[0]
                ad_frame = None
                for f in page.frames:
                    if (ad["data_aa"] and ad["data_aa"] in f.url) or \
                       (src_key and src_key in f.url):
                        ad_frame = f
                        break
                if not ad_frame:
                    time.sleep(2)
                    for f in page.frames:
                        if (ad["data_aa"] and ad["data_aa"] in f.url) or \
                           (src_key and src_key in f.url):
                            ad_frame = f
                            break

                if ad_frame:
                    ad["loaded"] = True
                    ad["_frame"] = ad_frame
                    for sel in ["a", ".aa-title", ".aa-description", "h1,h2,h3", "p", "body"]:
                        try:
                            el_f = ad_frame.locator(sel).first
                            text = el_f.inner_text(timeout=2000).strip()
                            href = el_f.get_attribute("href") if sel == "a" else None
                            if text:
                                ad["text"]      = text[:200]
                                ad["click_url"] = href
                                break
                        except Exception:
                            continue
                    ad["text"] = ad["text"] or "<empty>"
                else:
                    ad["text"] = "<frame not found>"

                status = "✅" if ad["loaded"] else "❌"
                _print(f"   {status} [{ad['network']}] #{ad['data_aa'] or 'n/a'}")
                _print(f"      Text : {ad['text']}")
                if ad["click_url"]:
                    _print(f"      Link : {ad['click_url'][:80]}")

            # ── 4. CLICK — click via frame context, handle same-tab nav ──────────
            _print("\n── 4. CLICK ──────────────────────────────────────────")
            clickable = [a for a in detected_ads if a["loaded"]]
            if not clickable:
                _print("   ⏭  No loaded ads to click")
            for target in clickable:
                try:
                    # scroll away briefly then back — mimics real user behaviour
                    page.mouse.wheel(0, random.randint(200, 500))
                    time.sleep(random.uniform(0.8, 1.5))
                    page.mouse.wheel(0, -random.randint(200, 500))
                    time.sleep(random.uniform(0.5, 1.0))

                    selector = f"iframe[data-aa='{target['data_aa']}']" if target["data_aa"] \
                               else f"iframe[src*='{target['src'].lstrip('/').split('?')[0][:40]}']"
                    el = page.locator(selector).first
                    el.scroll_into_view_if_needed(timeout=5000)
                    time.sleep(random.uniform(1.0, 2.5))

                    bb = el.bounding_box()
                    if not bb:
                        _print(f"   ⚠  No bounding box for #{target['data_aa'] or 'n/a'}")
                        continue

                    # Bézier approach from current mouse pos
                    cx = bb["x"] + bb["width"]  * random.uniform(0.3, 0.7)
                    cy = bb["y"] + bb["height"] * random.uniform(0.3, 0.7)
                    cur_x = bb["x"] + random.randint(-100, -30)
                    cur_y = bb["y"] + random.randint(-60, -10)
                    _bezier_move(page, cur_x, cur_y, cx, cy)
                    time.sleep(random.uniform(0.2, 0.6))

                    # click via the frame's <a> element — not the outer iframe bbox
                    clicked_via_frame = False
                    if target.get("_frame"):
                        try:
                            a_el = target["_frame"].locator("a").first
                            prev_url = page.url
                            try:
                                with context.expect_page(timeout=6000) as nti:
                                    a_el.click(timeout=4000)
                                new_tab = nti.value
                            except Exception:
                                # same-tab navigation or no nav
                                new_tab = None
                                if page.url != prev_url:
                                    _print(f"   🖱  Clicked (same-tab nav) #{target['data_aa'] or 'n/a'} [{target['network']}]")
                                    time.sleep(random.uniform(4.0, 8.0))
                                    page.go_back(wait_until="domcontentloaded", timeout=10000)
                                else:
                                    _print(f"   🖱  Clicked (no nav) #{target['data_aa'] or 'n/a'} [{target['network']}]")
                                    time.sleep(random.uniform(2.0, 4.0))
                                clicked_via_frame = True
                            if new_tab:
                                clicked_via_frame = True
                        except Exception:
                            pass

                    if not clicked_via_frame:
                        # fallback: raw mouse click on iframe bbox
                        prev_url = page.url
                        try:
                            with context.expect_page(timeout=8000) as nti:
                                page.mouse.click(cx, cy)
                            new_tab = nti.value
                        except Exception:
                            new_tab = None
                            if page.url != prev_url:
                                _print(f"   🖱  Clicked (same-tab nav) #{target['data_aa'] or 'n/a'} [{target['network']}]")
                                time.sleep(random.uniform(4.0, 8.0))
                                page.go_back(wait_until="domcontentloaded", timeout=10000)
                            else:
                                _print(f"   🖱  Clicked (no nav) #{target['data_aa'] or 'n/a'} [{target['network']}]")
                                time.sleep(random.uniform(2.0, 4.0))
                            continue

                    if new_tab:
                        _print(f"   🖱  Clicked ad #{target['data_aa'] or 'n/a'} [{target['network']}]")
                        try:
                            new_tab.wait_for_load_state("domcontentloaded", timeout=20000)
                        except Exception:
                            pass

                        # extra wait if page title is still loading
                        for _ in range(6):
                            try:
                                t = new_tab.title()
                                if t and t not in ("", "about:blank"):
                                    break
                            except Exception:
                                pass
                            time.sleep(2)

                        # wait for networkidle — page fully settled
                        try:
                            new_tab.wait_for_load_state("networkidle", timeout=15000)
                        except Exception:
                            pass

                        tab_url   = new_tab.url
                        tab_title = "<unknown>"
                        tab_text  = "<none>"
                        try: tab_title = new_tab.title()
                        except Exception: pass
                        try: tab_text = new_tab.locator("h1, h2, h3, p").first.inner_text(timeout=5000).strip()[:200]
                        except Exception: pass

                        _print(f"   🆕 New tab:")
                        _print(f"      URL   : {tab_url[:100]}")
                        _print(f"      Title : {tab_title}")
                        _print(f"      Text  : {tab_text}")
                        target["landing_url"]   = tab_url
                        target["landing_title"] = tab_title
                        target["landing_text"]  = tab_text

                        # scroll landing page during dwell — mimics reading
                        dwell = random.uniform(12.0, 22.0)
                        _print(f"   ⏱  Dwelling {dwell:.1f}s...")
                        third = dwell / 3
                        time.sleep(third)
                        try: new_tab.mouse.wheel(0, random.randint(300, 600))
                        except Exception: pass
                        time.sleep(third)
                        try: new_tab.mouse.wheel(0, random.randint(200, 500))
                        except Exception: pass
                        time.sleep(dwell - third * 2)
                        new_tab.close()
                        _print("   ✅ Tab closed")

                except Exception as e:
                    _print(f"   ⚠  Click failed for #{target['data_aa'] or 'n/a'}: {e}")

            # ── single log write at end with all data ─────────────────────
            with open(AD_LOG_FILE, "a") as f:
                serializable_ads = [{k: v for k, v in ad.items() if k != "_frame"} for ad in detected_ads]
                f.write(json.dumps({
                    "ts":      time.strftime("%Y-%m-%d %H:%M:%S"),
                    "session": session_id,
                    "ip":      ip_info.get("ip"),
                    "cc":      cc,
                    "device":  "mobile" if is_mobile else "desktop",
                    "ads":     serializable_ads,
                }) + "\n")

            _print("─────────────────────────────────────────────────────\n")
            time.sleep(random.uniform(5.0, 10.0))
            _print(f"Current URL: {page.url}")



        finally:
            context.close()


# --- entry point ---
if not _args.tor and not _args.proxy:
    print("[-] Specify -T (Tor) or -P (proxy). Exiting.")
    sys.exit(1)

proxy_config = None
if _args.proxy:
    with open(PROXY_FILE) as f:
        proxies = [l.strip() if "://" in l.strip() else f"http://{l.strip()}" for l in f if l.strip()]
    if not proxies:
        print("[~] proxies.txt empty — restoring from used_proxies.txt")
        with open(USED_PROXY_FILE) as f:
            proxies = [l.strip() for l in f if l.strip()]
        with open(PROXY_FILE, "w") as f:
            f.write("\n".join(proxies) + "\n")
        open(USED_PROXY_FILE, "w").close()
        print(f"[~] Restored {len(proxies)} proxies")
    proxy_url = get_working_proxy(proxies)
    mark_proxy_used(proxy_url)
    print(f"[~] Using proxy: {proxy_url}")
    proxy_config = {"server": proxy_url}

name, email_addr = generate_identity()
session_id = _args.socks_port
if _args.debug:
    run_session(elements={"email": email_addr, "name": name}, session_id=session_id, proxy_config=proxy_config)
else:
    with Xvfb(width=1920, height=1080, colordepth=24):
        run_session(elements={"email": email_addr, "name": name}, session_id=session_id, proxy_config=proxy_config)
