import psycopg2

## Bu değeri localinde çalışırken kendi passwordün yap. Ama kodu pushlarken 'postgres' olarak bırak.
password = 'postgres'


def connect_db():
    conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="postgres",
    user="postgres",
    password=password)
    return conn


def question_1_query():
    connection = connect_db()
    cursor = connection.cursor()
    cursor.execute('SET search_path TO db_ds')
    cursor.execute('SELECT * FROM students WHERE age > 22')
    data = cursor.fetchall()
    cursor.close()
    connection.close()
    return data


def question_2_query():
    connection = connect_db()
    cursor = connection.cursor()
    cursor.execute('SET search_path TO db_ds')
    cursor.execute("SELECT * FROM courses WHERE category = 'Veritabanı'")
    data = cursor.fetchall()
    cursor.close()
    connection.close()
    return data


def question_3_query():
    connection = connect_db()
    cursor = connection.cursor()
    cursor.execute('SET search_path TO db_ds')
    cursor.execute("select * from students where first_name like'A%'")
    data = cursor.fetchall()
    cursor.close()
    connection.close()
    return data


def question_4_query():
    connection = connect_db()
    cursor = connection.cursor()
    cursor.execute('SET search_path TO db_ds')
    cursor.execute("select * from courses where course_name like'%SQL%'")
    data = cursor.fetchall()
    cursor.close()
    connection.close()
    return data


def question_5_query():
    connection = connect_db()
    cursor = connection.cursor()
    cursor.execute('SET search_path TO db_ds')
    cursor.execute("SELECT * FROM students WHERE age between 22 and 24;")
    data = cursor.fetchall()
    cursor.close()
    connection.close()
    return data


def question_6_query():
    connection = connect_db()
    cursor = connection.cursor()
    cursor.execute('SET search_path TO db_ds')
    cursor.execute("""select s.first_name, s.last_name from students as s
    left join enrollments as e
    on e.student_id=s.student_id
    where e.course_id is not null""")
    data = cursor.fetchall()
    cursor.close()
    connection.close()
    return data


def question_7_query():
    connection = connect_db()
    cursor = connection.cursor()
    cursor.execute('SET search_path TO db_ds')
    cursor.execute(
    """select c.course_name, count(e.student_id) as student_count
     from courses as c
     join enrollments e on c.course_id = e.course_id
     where c.category = 'Veritabanı'
     group by c.course_name;""")
    data = cursor.fetchall()
    cursor.close()
    connection.close()
    return data


def question_8_query():
    connection = connect_db()
    cursor = connection.cursor()
    cursor.execute('SET search_path TO db_ds')
    cursor.execute(
        """select c.course_name, i.name from courses as c
    left join course_instructors as ci
    on c.course_id=ci.course_id
    left join instructors as i
    on i.instructor_id=ci.instructor_id""")
    data = cursor.fetchall()
    cursor.close()
    connection.close()
    return data


def question_9_query():
    connection = connect_db()
    cursor = connection.cursor()
    cursor.execute('SET search_path TO db_ds')
    cursor.execute("""
    select * from students as s
    left join enrollments as e 
    on s.student_id=e.student_id
    where e.student_id is null;""")
    data = cursor.fetchall()
    cursor.close()
    connection.close()
    return data


def question_10_query():
    connection = connect_db()
    cursor = connection.cursor()
    cursor.execute('SET search_path TO db_ds')
    cursor.execute("""select c.course_name, avg(s.age) as avg_age
    from students as s
    left join enrollments as e
    on s.student_id=e.student_id
    left join courses as c 
    on e.course_id=c.course_id
    group by c.course_name""")
    data = cursor.fetchall()
    cursor.close()
    connection.close()
    return data


def question_11_query():
    connection = connect_db()
    cursor = connection.cursor()
    cursor.execute('SET search_path TO db_ds')
    cursor.execute("""select s.first_name, s.last_name, count(e.course_id) as total_courses
    from students as s
    left join enrollments as e
    on s.student_id=e.student_id
    group by s.first_name, s.last_name""")
    data = cursor.fetchall()
    cursor.close()
    connection.close()
    return data


def question_12_query():
    connection = connect_db()
    cursor = connection.cursor()
    cursor.execute('SET search_path TO db_ds')
    cursor.execute("""select i.name, count(ci.course_id) as total_courses
    from instructors as i
    left join course_instructors as ci
    on ci.instructor_id=i.instructor_id
    group by i.name
    having count(ci.course_id) > 1;""")
    data = cursor.fetchall()
    cursor.close()
    connection.close()
    return data


def question_13_query():
    connection = connect_db()
    cursor = connection.cursor()
    cursor.execute('SET search_path TO db_ds')
    cursor.execute("""select c.course_name,  count(s.student_id) as unique_students 
    from courses as c
    left join enrollments as e 
    on c.course_id = e.course_id
    left join students as s 
    on s.student_id = e.student_id
    GROUP BY c.course_name;""")
    data = cursor.fetchall()
    cursor.close()
    connection.close()
    return data


def question_14_query():
    connection = connect_db()
    cursor = connection.cursor()
    cursor.execute('SET search_path TO db_ds')
    cursor.execute("""select s.first_name, s.last_name from students as s
    left join enrollments as e 
    on s.student_id=e.student_id
    left join courses as c
    on c.course_id=e.course_id
    where course_name IN ('SQL Temelleri', 'İleri SQL')""")
    data = cursor.fetchall()
    cursor.close()
    connection.close()
    return data


def question_15_query():
    connection = connect_db()
    cursor = connection.cursor()
    cursor.execute('SET search_path TO db_ds')
    cursor.execute("""select s.first_name, s.last_name, c.course_name, i.name, e.enrollment_date
    from students as s
    left join enrollments as e
    on s.student_id=e.student_id
    left join courses as c
    on e.course_id=c.course_id
    left join course_instructors as ci
    on ci.course_id=c.course_id
    left join instructors as i
    on ci.instructor_id=i.instructor_id""")
    data = cursor.fetchall()
    cursor.close()
    connection.close()
    return data