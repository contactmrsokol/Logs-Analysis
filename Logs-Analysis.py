#!/usr/bin/env python3
import psycopg2


def popular_articles():
    """Print top 3 popular articles."""
    db = psycopg2.connect("dbname=news")
    c = db.cursor()
    query = """SELECT title, count(title) as views
               FROM articles,log
               WHERE log.path = concat('/article/',articles.slug)
               GROUP BY title
               ORDER BY views DESC
               LIMIT 3;"""
    c.execute(query)
    results = c.fetchall()
    db.close()
    print ("\nPopular Articles:\n")
    for i in range(0, len(results), 1):
        print "\"" + results[i][0] + "\" - " + str(results[i][1]) + " views"


def popular_authors():
    """Print most popelar authors."""
    db = psycopg2.connect("dbname=news")
    c = db.cursor()
    query = """SELECT authors.name, count(articles.author) as views
               FROM articles, log, authors
               WHERE log.path = concat('/article/',articles.slug) and
               articles.author = authors.id
               GROUP BY authors.name
               ORDER BY views DESC"""
    c.execute(query)
    results = c.fetchall()
    db.close()
    print ("\nPopular Authors:\n")
    for i in range(0, len(results), 1):
        print results[i][0] + " - " + str(results[i][1]) + " views"


def error_log():
    """Print error log."""
    db = psycopg2.connect("dbname=news")
    c = db.cursor()
    query = """SELECT Date, Total, Error, (Error::float*100)/Total::float as Percent
               FROM (
               SELECT to_char(time::timestamp::date, 'Month DD, YYYY') as Date,
               count(status) as Total, sum(case when status =
               '404 NOT FOUND' then 1 else 0 end) as Error
               FROM log
               GROUP BY time::timestamp::date) as result
               WHERE (Error::float*100)/Total::float > 1.0;"""
    c.execute(query)
    results = c.fetchall()
    db.close()
    print ("\nDays on which more than 1% of requests lead to errors:\n")
    for i in range(0, len(results), 1):
        print(str(results[i][0]) +
              " - " + str(round(results[i][3], 1)) + "% errors")
    print ("\n")


if __name__ == '__main__':
    popular_articles()
    popular_authors()
    error_log()
