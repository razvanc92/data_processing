import folium


class MapHelper(object):
    def __init__(self, sensors):
        self._center_point = [sensors[0][1], sensors[0][2]]
        self._sensors = sensors
        self._m = folium.Map(location=self._center_point, zoom_start=12)

    def draw_sensor(self, ids=None, icon=0):
        if ids is None:
            sensors = self._sensors
        else:
            sensors = self._sensors[ids]

        for sensor in sensors:
            icon_legend = [
                folium.Icon(icon='cloud', color='red'),
                folium.Icon(icon='cloud', color='pink'),
                folium.Icon(icon='cloud', color='green'),
                folium.Icon(icon='cloud', color='black'),
                folium.Icon(icon='cloud')
            ]

            folium.Marker(
                icon=icon_legend[icon],
                location=[sensor[1], sensor[2]],
                popup='Id: {0}, Location: {1}'.format(int(sensor[0]),(sensor[1], sensor[2])),
            ).add_to(self._m)
        self._save()

    def draw_line(self, points):
        folium.PolyLine(points, color="red", weight=2.5, opacity=1).add_to(self._m)

    def _save(self):
        self._m.save('index.html')
