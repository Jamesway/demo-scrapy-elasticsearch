
import random, datetime

# generate a random number of appointments for a practice
def apppointment_calc(apps_to_generate = 6):


    # 9a-5p
    app_hours = ['09', '10', '11', '13', '14', '15', '16', '17']
    app_minutes = ['00', '15', '30', '45']

    appt_times = []

    hours  = random.sample(app_hours, apps_to_generate)
    for h in hours:
        for m in random.sample(app_minutes, random.randrange(1, len(app_minutes))):
            timestamp = datetime.datetime.strptime('2018 07 05 ' + str(h) + ':' + str(m), '%Y %m %d  %H:%M')
            appt_times.append(timestamp)

    return appt_times