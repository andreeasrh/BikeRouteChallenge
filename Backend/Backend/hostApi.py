from flask import Flask, jsonify, request, session
import requests
import datetime
from statistics import mean
from flask_cors import CORS
import numpy as np
from dotenv import load_dotenv
import os
import mysql.connector
import json
import random
import time
from datetime import datetime
import io
import base64
from itertools import tee
from testPointsInSq import getInfoAboutIndex
import hashlib

key = ''
key2 = ''

bikeRouteApi = Flask(__name__)

CORS(bikeRouteApi)

load_dotenv()

mydb = mysql.connector.connect(
    host="localhost",
    user=os.getenv("DB_username"),
    password=os.getenv("DB_password"),
    database=os.getenv("DB_database")
)

cursor = mydb.cursor()
cursor2 = mydb.cursor()


windSpeed = []
windDeg = []
temp = []
tempMin = []
tempMax = []
humidity = []
visibility = []
pressure = []
gpxZoneArray = []
gpxZoneJson = {}
data = []
dataForSectionArray = []

sunset = 0
sunrise = 0
sections = 0


def infoHourlyPoint(response):
    data = response.json()

    infoMain = {
        'lat': data['lat'],
        'lon': data['lon']
    }

    h = 0
    allInfo = {}

    for hour in data['hourly']:
        real_time = datetime.utcfromtimestamp(hour['dt'])
        real_timeParse = real_time.strftime('%Y-%m-%d %H:%M:%S')
        visibility = hour['visibility']
        temp = round(hour['temp'] - 273.15, 2)
        tempFeels = round(hour['feels_like'] - 273.15, 2)
        hum = hour['humidity']
        windSpeed = round(hour['wind_speed'], 2)

        info = {
            'h': h,
            'time': real_timeParse,
            'visibility': visibility,
            'temp': temp,
            'tempfeels': tempFeels,
            'hum': hum,
            'windSpeed': windSpeed
        }

        if h == 10:
            break
        else:
            allInfo[h] = info

        h += 1

    infoMain['hourly'] = allInfo
    return infoMain


def processGpxData():
    with open('allCoord.json', 'r') as file:
        gpx_data = json.load(file)

    API_KEY = ''
    processed_data = []

    for point_data in gpx_data:
        center_lat = point_data.get('centerLat')
        center_lon = point_data.get('centerLon')

        url = f'https://api.openweathermap.org/data/3.0/onecall?lat={center_lat}&lon={center_lon}&appid={API_KEY}'
        res = requests.get(url)
        processed_data.append(infoHourlyPoint(res))
        print(
            f'S A FACUT REQ PENTRU POINT {point_data["indexI"]}, {point_data["indexJ"]}')
        time.sleep(1)

    with open('processedData.json', 'w') as processed_file:
        json.dump(processed_data, processed_file)


def getWindScore(minScore, maxScore, minWind, maxWind, currentWind):
    currentWind = max(minWind, min(currentWind, maxWind))

    if minScore == 1 and maxScore == 1.2:
        return np.clip(minScore + (1*0.1) * ((currentWind - minWind) / (maxWind - minWind))**(2*0.9), minScore, maxScore)
    else:
        return np.clip(maxScore - (1) * ((currentWind - minWind) / (maxWind - minWind))**(2*0.27), minScore, maxScore)


def getSectionArrayFcn(data):
    section = 0
    sectionArray = []
    coordonatePunct = []

    allgpxpoints = data.get('fullGpx', [])  # [{}]
    redPoints = data.get('weatherPoints', [])  # [{}]

    windSpeed = data.get('windSpeed', [])   # [{}]
    windDeg = data.get('windDeg', [])   # [{}]
    directionDeg = data.get('directionDeg', [])   # [{}]

    frontWindSpeed = float(data.get('frontWindSpeed'))
    maxSideDeg = float(data.get('maxSideDeg'))
    sideWindSpeed = float(data.get('sideWindSpeed'))

    start = data.get('start')
    end = data.get('end')

    for point in allgpxpoints:
        if section < len(redPoints):
            punct = {
                'lat': point['lat'],
                'lon': point['lon']
            }

            coordonatePunct.append(punct)

            score = 0

            windSpeedSection = windSpeed[section]  # float
            windDegSection = windDeg[section]  # float
            directionDegSection = directionDeg[section]  # float

            diff = abs(directionDegSection - windDegSection)

            if (windSpeedSection > kmpfromMps(frontWindSpeed)) and diff < 10:
                score = getWindScore(0, 0.5, start, end, windSpeedSection)
            elif (diff > kmpfromMps(maxSideDeg)) and windSpeedSection > kmpfromMps(sideWindSpeed):
                score = getWindScore(0.6, 0.8, start, end,
                                     windSpeedSection)
            elif (windSpeedSection < kmpfromMps(frontWindSpeed)):
                score = getWindScore(0.9, 1, start, end, windSpeedSection)
            elif (windDegSection < directionDegSection and diff > 30):
                score = getWindScore(1.01, 1.2, start, end,
                                     windSpeedSection)
            elif windSpeedSection > end:
                score = -1

            sectionData = {
                'section': section,
                'sectionScore': score,
                'array': coordonatePunct.copy()
            }

            sectionArray.append(sectionData)

            if (redPoints[section].get('lat') == point['lat'] and redPoints[section].get('lon') == point['lon']):
                section += 1
                coordonatePunct.clear()
    return sectionArray


def kmpfromMps(val) -> float:
    return (val * 1000) / 3600


@bikeRouteApi.route('/createSectionArray', methods=['POST'])
def createSectionArray():
    global dataForSectionArray
    dataForSectionArray = request.get_json()
    return jsonify({'msg': 'Data received.'})


@bikeRouteApi.route('/getSectionArray', methods=['GET'])
def getSectionArray():
    global dataForSectionArray
    if len(dataForSectionArray) > 0:
        sectionArray = getSectionArrayFcn(dataForSectionArray)
        return jsonify(sectionArray), 200
    else:
        return jsonify({'msg': 'Data problem'}), 500


def processingArray(array):
    processed_data = []

    for item in array:
        section = item['section']
        lat = item['lat']
        lon = item['lon']

        processed_item = {
            'section': section,
            'lat': lat,
            'lon': lon,
        }

        processed_data.append(processed_item)

    return jsonify(processed_data)


def processingPath(pathList):
    global sunset, sunrise, sections, temp, tempMin, tempMax, humidity, visibility, pressure, windSpeed, windDeg
    temp, tempMin, tempMax, humidity, visibility, pressure, windSpeed, windDeg
    for section, lat, lon in pathList:
        url = f'http://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={key}'
        response = requests.get(url)
        if response.status_code == 200:
            sections = section
            data = response.json()
            sunset = data["sys"]["sunset"]
            sunrise = data["sys"]["sunrise"]
            temp.append(data["main"]["temp"] - 273.15)
            tempMin.append(data["main"]["temp_min"] - 273.15)
            tempMax.append(data["main"]["temp_max"] - 273.15)
            humidity.append(data["main"]["humidity"])
            visibility.append(data.get("visibility", 0))
            pressure.append(data["main"]["pressure"])
            windSpeed.append(data["wind"]["speed"])
            windDeg.append(data["wind"]["deg"])


@bikeRouteApi.route('/setGpxPointsFile', methods=['POST'])
def setGpxPointsFile():
    mapName = request.form['mapName']
    data = request.form['file']
    userId = -1
    username = request.form['username']
    with mydb.cursor() as cursor:
        sqlQuery = "SELECT id FROM users WHERE username = %s;"
        cursor.execute(sqlQuery, (username,))
        userId = cursor.fetchall()[0][0]

        sqlQuery = "UPDATE gpxinfo SET gpx_points = %s WHERE mapName = %s AND userId = %s;"
        cursor.execute(sqlQuery, (data, mapName, userId,))
        mydb.commit()
    with open('gpxPoints.json', 'w') as file:
        json.dump(data, file)
    return jsonify({'message': 'Datele au fost primite.', 'data': data}), 200


def isPointinSq(point, sq, pltShow):
    pointLat = point['lat']
    pointLon = point['lon']

    sqCenterLat = sq['centerLat']
    sqCenterLon = sq['centerLon']
    sqTopLeftLat = sq['topLeftLat']
    sqTopLeftLon = sq['topLeftLng']
    sqBotRightLat = sq['bottomRightLat']
    sqBotRightLon = sq['bottomRightLon']

    sqTopRightLat = sq['topLeftLat']
    sqTopRightLon = sq['bottomRightLon']

    sqBotLeftLat = sq['bottomRightLat']
    sqBotLeftLon = sq['topLeftLng']

    if sqBotRightLat < pointLat < sqTopLeftLat and sqTopLeftLon < pointLon < sqTopRightLon:
        if pltShow:
            print('1')
        return True
    return False


def getIndex4Points(gpxPoints, sqInfo, gpxData):
    iDorit = []
    jDorit = []

    centerLat = []
    centerLon = []
    topLeftLat = []
    topLeftLon = []
    bottomRightLat = []
    bottomRightLon = []

    h = 0

    ctime = datetime.now()
    firstElement = list(gpxData.values())[0][0]['data']['hourly']
    for sh in firstElement:
        time = datetime.strptime(
            firstElement[str(sh)]['time'], "%Y-%m-%d %H:%M:%S")
        if time > ctime:  # Aici e mai mic
            h = int(sh)
        else:
            break

    for point in gpxPoints:
        for sq in sqInfo:
            if isPointinSq(point, sq, False):
                iDorit.append(sq['indexI'])
                jDorit.append(sq['indexJ'])
                centerLat.append(sq['centerLat'])
                centerLon.append(sq['centerLon'])
                bottomRightLat.append(sq['bottomRightLat'])
                bottomRightLon.append(sq['bottomRightLon'])
                topLeftLat.append(sq['topLeftLat'])
                topLeftLon.append(sq['topLeftLng'])

    return iDorit, jDorit, h, centerLat, centerLon, topLeftLat, topLeftLon, bottomRightLat, bottomRightLon


def pair(iter):
    a, b = tee(iter)
    next(b, None)
    return zip(a, b)


def degPositionFunction(point, nextPoint):
    lat1Rad = np.deg2rad(float(point['lat']))
    lon1Rad = np.deg2rad(float(point['lon']))
    lat2Rad = np.deg2rad(float(nextPoint['lat']))
    lon2Rad = np.deg2rad(float(nextPoint['lon']))

    deltaLon = lon2Rad - lon1Rad

    angle = np.rad2deg((np.arctan2(
        np.sin(deltaLon) * np.cos(lat2Rad),
        np.cos(lat1Rad) * np.sin(lat2Rad) - np.sin(lat1Rad) *
        np.cos(lat2Rad) * np.cos(deltaLon)
    )))

    angle = (90 - angle + 360) % 360
    return angle


@bikeRouteApi.route('/getGpxWeatherDataFile', methods=['GET'])
def getGpxWeather():
    try:
        mapName = request.args.get('mapName')
        with mydb.cursor() as cursor:
            sqlQuery = "SELECT gpx_processed FROM gpxinfo WHERE mapName = %s;"
            cursor.execute(sqlQuery, (mapName,))
            gpxProcessed = cursor.fetchall()

            if not gpxProcessed:
                return jsonify({"error": "Nu exista niciun fisier pentru acest mapName"}), 404

            gpxProcessed = json.loads(gpxProcessed[0][0])
            return jsonify(gpxProcessed), 200
    except Exception as e:
        return jsonify({"error": "A aparut o eroare " + str(e)}), 500


@bikeRouteApi.route('/getGpxPointsFile', methods=['GET'])
def getGpxPoints():
    mapName = request.args.get('mapName')
    with mydb.cursor() as cursor:
        sqlQuery = "SELECT gpx_points FROM gpxinfo WHERE mapName = %s;"
        cursor.execute(sqlQuery, (mapName,))
        gpxProccessed = cursor.fetchall()
        gpxProccessed = json.loads(gpxProccessed[0][0])
        return jsonify(gpxProccessed), 200


@bikeRouteApi.route('/processGpxFile', methods=['POST'])
def openGpxSavedData2():
    file = 'mergedProcessedData.json'
    mapName = request.form['mapName']

    with open(file, "r") as file:
        gpxData = json.load(file)

    with mydb.cursor() as cursor:
        sqlQuery = "SELECT gpx_points FROM gpxinfo WHERE mapName = %s;"
        cursor.execute(sqlQuery, (mapName,))
        gpxPointsDb = cursor.fetchall()[0][0]
        with open("gpxPoints.json", "w") as file:
            file.write(gpxPointsDb)

    with open("gpxPoints.json", "r") as file:
        gpxPoints = json.load(file)

    with open("allCoord.json", "r") as sqFile:
        sqInfo = json.load(sqFile)

    iDorit, jDorit, h, centerLat, centerLon, topLeftLat, topLeftLon, bottomRightLat, bottomRightLon = getIndex4Points(
        gpxPoints, sqInfo, gpxData)

    info = []
    degPosition = []
    for (point, nextPoint), i, j, centerLat, centerLon, topLeftLat, topLeftLon, bottomRightLat, bottomRightLon in zip(pair(gpxPoints), iDorit, jDorit, centerLat, centerLon, topLeftLat, topLeftLon, bottomRightLat, bottomRightLon):
        degPosition.append(np.round(degPositionFunction(point, nextPoint), 2))
        sq = {
            'indexI': i,
            'indexJ': j,
            'centerLat': centerLat,
            'centerLon': centerLon,
            'topLeftLat': topLeftLat,
            'topLeftLon': topLeftLon,
            'bottomRightLat': bottomRightLat,
            'bottomRightLon': bottomRightLon
        }
        main = {
            'lat': point['lat'],
            'lon': point['lon'],
            'degPosition': degPosition[-1],
            'hPoint': point['h'],
            'hTime': point['time'],
            'sq': sq,
            'data': getInfoAboutIndex(i, j, h, gpxData, degPosition[-1])
        }
        info.append(main)

    info_json = json.dumps(info)
    print(info_json)
    with open('finalInfo.json', 'w') as file:
        file.write(info_json)

    with mydb.cursor() as cursor:
        sqlQuery = "UPDATE gpxinfo SET gpx_processed = %s WHERE mapName = %s;"
        cursor.execute(sqlQuery, (info_json, mapName,))
        mydb.commit()
        return jsonify('ok'), 200


@bikeRouteApi.route('/getlastgpxfile/<string:username>', methods=['GET'])
def getLastGpxFile(username):
    userId = -1
    print(username)
    limitMaps = 100
    try:
        with mydb.cursor() as cursor:

            sqlQuery = "SELECT id FROM users WHERE username = %s;"
            cursor.execute(sqlQuery, (username,))
            userId = cursor.fetchall()[0][0]

            sqlQuery = "SELECT gpx_info, map_id, mapName FROM gpxinfo WHERE map_id IN (SELECT gpx_id FROM gpxmaps WHERE id = %s) ORDER BY map_id DESC LIMIT %s;"
            cursor.execute(sqlQuery, (userId, limitMaps,))
            gpx = cursor.fetchall()

            if not gpx:
                return jsonify({'error': 'Nu exista date in db.'}), 404

            gpxInfo = {}
            i = 0
            while i < len(gpx) and i < limitMaps:
                print(i)

                mapId = gpx[i][1]
                gpxFile = gpx[i][0].replace('\r\n', '')

                gpxData = {
                    'mapId': mapId,
                    'mapName': gpx[i][2],
                    'gpx': gpxFile

                }

                gpxInfo[i] = gpxData
                i += 1
            return jsonify(gpxInfo), 200

    except mysql.connector.Error as err:
        print("Eroare in baza de date:", err)
        return jsonify({'error': 'Eroare in baza de date'}), 500


@bikeRouteApi.route('/deletegpxmap/<int:user_id>/<string:mapName>', methods=['DELETE'])
def deleteGpxMap(user_id, mapName):
    print(user_id, mapName)
    try:
        with mydb.cursor() as cursor:
            sqlQuery = "SELECT gpx_id FROM gpxmaps WHERE id = %s AND gpx_id IN (SELECT map_id FROM gpxinfo WHERE mapName = %s)"
            cursor.execute(sqlQuery, (user_id, mapName))
            result = cursor.fetchall()
            print(result)
            if result is None:
                return jsonify({'error': 'Harta nu exista'}), 404

            gpx_id = result[0]

            sqlQuery = "DELETE FROM gpxinfo WHERE mapName = %s AND map_id = %s"
            cursor.execute(sqlQuery, (mapName, gpx_id))

            sqlQuery = "DELETE FROM gpxmaps WHERE id = %s AND gpx_id = %s"
            cursor.execute(sqlQuery, (user_id, gpx_id))

            mydb.commit()

            return jsonify({'message': 'Harta a fost ștearsă cu succes'}), 200
    except mysql.connector.Error as err:
        print("Eroare în baza de date:", err)
        return jsonify({'error': 'Eroare în baza de date'}), 500


@bikeRouteApi.route('/addarraydata', methods=['POST'])
def GetArray():
    global gpxZoneArray, gpxZoneJson
    gpxZoneArray = []
    data = request.get_json()
    gpxZoneArray = data.get('gpxZoneArray', [])
    return jsonify(gpxZoneJson)


@bikeRouteApi.route('/getarrayweatherdata', methods=['GET'])
def GetWeather():
    global gpxZoneJson
    return jsonify(gpxZoneJson)


@bikeRouteApi.route('/addgpxfile', methods=['POST'])
def addGpxFile():
    try:
        userId = -1
        username = request.form['username']
        gpxFile = request.files['file']
        mapName = request.form['mapName']

        with mydb.cursor() as cursor:
            sqlQuery = "SELECT id FROM users WHERE username = %s;"
            cursor.execute(sqlQuery, (username,))
            userId = cursor.fetchall()[0][0]

            sqlQuery = "INSERT INTO `gpxmaps` (`id`) VALUES (%s);"
            cursor.execute(sqlQuery, (userId,))
            mydb.commit()
            gpx_id = cursor.lastrowid

            sqlQuery2 = "INSERT INTO `gpxinfo` (map_id, gpx_info, mapName, userId) VALUES (%s,%s, %s, %s)"
            cursor.execute(
                sqlQuery2, (gpx_id, gpxFile.read(), mapName, userId,))
            mydb.commit()

        return jsonify({'message': 'Datele au fost primite.'}), 200
    except Exception as e:
        return jsonify({'message': 'Erroare', 'error': str(e)}), 500


@bikeRouteApi.route('/addBestScoreInDb', methods=['POST'])
def getBestScore():
    date = request.form.get('date')
    map = str(request.form.get('mapName'))
    try:
        lenBestScores = 5
        with mydb.cursor() as cursor:
            sqlQuery = "SELECT gpx_processed FROM gpxinfo WHERE mapName = %s;"
            cursor.execute(sqlQuery, (map,))
            gpx_data = cursor.fetchall()
            gpx_data = json.loads(gpx_data[0][0])

            sqlQuery2 = "SELECT gpx_points FROM gpxinfo WHERE mapName = %s;"
            cursor.execute(sqlQuery2, (map,))
            gpx_Points = cursor.fetchall()
            gpx_Points = json.loads(gpx_Points[0][0])

        bestScore = 0
        time = ''
        l = 40
        pointsLen = len(gpx_Points)
        scoresInfo = []
        print(pointsLen)
        for h in range(0, l):
            scoreAv = 0
            popAv = 0
            dtime = ''
            hPoint = 0
            saved = False
            for point in gpx_data:
                score = point['data'][h]['score']
                pop = point['data'][h]['pop']
                time = point['data'][h]['time']

                if time < date:
                    continue

                if saved == False:
                    dtime = time
                    saved = True

                if point['hPoint'] > hPoint:
                    hPoint = point['hPoint']
                    h += 1

                if score == None:
                    score = 0

                scoreAv += score
                popAv += pop

            popAv = popAv / pointsLen
            scoreAv = np.round(
                ((scoreAv / pointsLen) * 0.8 + (popAv) * 0.2)*100, 2)

            if score is not None and score > bestScore:
                bestScore = score*0.8 + pop*0.2

            if scoreAv != 0 and lenBestScores >= 0:
                if bestScore == 0:
                    bestScore = scoreAv

                info = {'scoreAv': scoreAv,
                        'dtime': dtime}
                bestScore = 0
                scoresInfo.append(info)
                lenBestScores -= 1
        with mydb.cursor() as cursor:
            sqlQuery = "SELECT map_id FROM gpxinfo WHERE mapName = %s;"
            cursor.execute(sqlQuery, (map,))
            map_id = cursor.fetchall()[0][0]

            sqlQuery2 = "UPDATE gpxinfo SET scoresInfo = %s WHERE map_id = %s;"
            cursor.execute(sqlQuery2, (json.dumps(scoresInfo), map_id))
            mydb.commit()

        return jsonify(scoresInfo), 200
    except mysql.connector.Error as err:
        print("Eroare in baza de date:", err)
        return jsonify({'error': 'Eroare in baza de date'}), 500


@bikeRouteApi.route('/convertToken', methods=['GET'])
def convertToken():
    token = request.args.get('token')
    if token is None:
        return jsonify({'error': 'Tokenul nu a fost primit'}), 400
    print(hashlib.md5(token.encode()).hexdigest())
    return jsonify(hashlib.md5(token.encode()).hexdigest()), 200


@bikeRouteApi.route('/getBestScore', methods=['GET'])
def GetBestScore():
    try:
        mapsName = str(request.args.get('mapName'))[:-1].split(',')
        mapsScore = {}
        for map in mapsName:
            try:
                with mydb.cursor() as cursor:
                    sqlQuery = "SELECT scoresInfo FROM gpxinfo WHERE mapName = %s;"
                    cursor.execute(sqlQuery, (map,))
                    scoresInfo = cursor.fetchall()
                    mapsScore[map] = (
                        {'scoreInfo': json.loads(scoresInfo[0][0])})
            except mysql.connector.Error as err:
                print("Eroare in baza de date:", err)
                return jsonify({'error': 'Eroare in baza de date'}), 500
    except Exception as e:
        return jsonify({'error': 'A aparut o eroare ' + str(e)}), 500

    return jsonify(mapsScore), 200


############################

@bikeRouteApi.route('/login', methods=['POST'])
def login():
    username = request.form['username']
    print(username)
    password = request.form['password']
    print(password)
    token = str(datetime.now().year) + \
        str(datetime.now().month) + str(datetime.now().day)
    token += username
    token = hashlib.md5(token.encode()).hexdigest()

    cursor.execute("SELECT * FROM users WHERE username = %s AND userpass = %s",
                   (username, password))
    result = cursor.fetchone()
    if result:
        cursor.execute("UPDATE users SET token = %s WHERE username = %s",
                       (token, username))
        mydb.commit()
        return jsonify({"status": "Success", 'token': token}), 200
    else:
        return jsonify("Invalid username or password"), 401


@bikeRouteApi.route('/register', methods=['POST'])
def register():
    username = request.form['username']
    password = request.form['password']
    email = request.form['email']
    token = str(datetime.now().year) + \
        str(datetime.now().month) + str(datetime.now().day) + \
        str(username) + str(password)
    token = hashlib.md5(token.encode.encode()).hexdigest()
    cursor.execute("INSERT INTO users (username, userpass, email, token) VALUES (%s, %s, %s, %s)",
                   (username, password, email))
    mydb.commit()
    return jsonify({"status": "Success"}), 200


@bikeRouteApi.route('/', methods=['GET'])
def main():
    return jsonify(gpxZoneArray)


if __name__ == '__main__':
    bikeRouteApi.run(debug=True)
