CREATE TABLE track_point
(id INT,
tp_time DATE,
lat DOUBLE(9, 7),
lon DOUBLE(10, 7),
FOREIGN KEY (id) REFERENCES activities(id)
);
