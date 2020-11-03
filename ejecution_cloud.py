from __future__ import print_function
import argparse
import logging
from operator import add
from random import random

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')


def calculate_game(partitions, output_uri, game):

    def processToRDD(line):
        f = line.split(",")
        return f

    logger.info(
       "Init Program divided in %s partitions.", partitions)
    with SparkSession.builder.appName("Game").getOrCreate() as spark:
        games = spark.sparkContext.textFile("s3://steamgames/steam-200k.csv").map(processToRDD)
        target_game = '"'+game+'"'
        players = games\
                        .filter( lambda line: line[1] == target_game and line[2] == 'play') \
                        .map(lambda  x: (int(x[0]),float(x[3]))) \
                        .reduceByKey(add).collect()

        logger.info("%s videojuego target y cantidad de jugadores %i.", target_game, len(players))
        for (game, horas_totales) in players:
            print("Usuario : %i, ha jugado el juego %s un total de: %f horas totales" %(game, target_game,horas_totales))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--partitions', default=2, type=int,
        help="The number of parallel partitions to use when calculate program.")
    parser.add_argument(
        '--output_uri', help="The URI where output is saved, typically an S3 bucket.")
    parser.add_argument(
	'--game', help="The game who is search to find")
    args = parser.parse_args()

    calculate_game(args.partitions, args.output_uri,args.game)

