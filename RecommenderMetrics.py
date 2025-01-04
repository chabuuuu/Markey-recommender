import itertools
from surprise import accuracy
from collections import defaultdict

class RecommenderMetrics:

    @staticmethod
    def MAE(predictions):
        return accuracy.mae(predictions, verbose=False)

    @staticmethod
    def RMSE(predictions):
        return accuracy.rmse(predictions, verbose=False)

    @staticmethod
    def GetTopN(predictions, n=10, minimumRating=4.0):
        """Get the top N recommended courses for each user."""
        topN = defaultdict(list)

        for userID, courseID, actualRating, estimatedRating, _ in predictions:
            if estimatedRating >= minimumRating:
                topN[int(userID)].append((courseID, estimatedRating))

        for userID, ratings in topN.items():
            ratings.sort(key=lambda x: x[1], reverse=True)
            topN[int(userID)] = ratings[:n]

        return topN

    @staticmethod
    def HitRate(topNPredicted, leftOutPredictions):
        """Calculate the hit rate for the left-out predictions."""
        hits = 0
        total = 0

        for leftOut in leftOutPredictions:
            userID = leftOut[0]
            leftOutCourseID = leftOut[1]

            hit = any(int(leftOutCourseID) == int(courseID) for courseID, _ in topNPredicted[int(userID)])
            if hit:
                hits += 1

            total += 1

        return hits / total

    @staticmethod
    def CumulativeHitRate(topNPredicted, leftOutPredictions, ratingCutoff=0):
        """Calculate the cumulative hit rate."""
        hits = 0
        total = 0

        for userID, leftOutCourseID, actualRating, estimatedRating, _ in leftOutPredictions:
            if actualRating >= ratingCutoff:
                hit = any(int(leftOutCourseID) == int(courseID) for courseID, _ in topNPredicted[int(userID)])
                if hit:
                    hits += 1

                total += 1

        return hits / total

    @staticmethod
    def RatingHitRate(topNPredicted, leftOutPredictions):
        """Calculate the hit rate grouped by rating."""
        hits = defaultdict(float)
        total = defaultdict(float)

        for userID, leftOutCourseID, actualRating, estimatedRating, _ in leftOutPredictions:
            hit = any(int(leftOutCourseID) == int(courseID) for courseID, _ in topNPredicted[int(userID)])
            if hit:
                hits[actualRating] += 1

            total[actualRating] += 1

        for rating in sorted(hits.keys()):
            print(rating, hits[rating] / total[rating])

    @staticmethod
    def AverageReciprocalHitRank(topNPredicted, leftOutPredictions):
        """Calculate the average reciprocal hit rank."""
        summation = 0
        total = 0

        for userID, leftOutCourseID, actualRating, estimatedRating, _ in leftOutPredictions:
            hitRank = 0
            rank = 0
            for courseID, _ in topNPredicted[int(userID)]:
                rank += 1
                if int(leftOutCourseID) == int(courseID):
                    hitRank = rank
                    break
            if hitRank > 0:
                summation += 1.0 / hitRank

            total += 1

        return summation / total

    @staticmethod
    def UserCoverage(topNPredicted, numUsers, ratingThreshold=0):
        """Calculate the user coverage."""
        hits = 0
        for userID in topNPredicted.keys():
            hit = any(predictedRating >= ratingThreshold for _, predictedRating in topNPredicted[userID])
            if hit:
                hits += 1

        return hits / numUsers

    @staticmethod
    def Diversity(topNPredicted, simsAlgo):
        """Calculate the diversity of the recommendations."""
        n = 0
        total = 0
        simsMatrix = simsAlgo.compute_similarities()

        for userID in topNPredicted.keys():
            pairs = itertools.combinations(topNPredicted[userID], 2)
            for pair in pairs:
                course1 = pair[0][0]
                course2 = pair[1][0]
                innerID1 = simsAlgo.trainset.to_inner_iid(str(course1))
                innerID2 = simsAlgo.trainset.to_inner_iid(str(course2))
                similarity = simsMatrix[innerID1][innerID2]
                total += similarity
                n += 1

        S = total / n
        return 1 - S

    @staticmethod
    def Novelty(topNPredicted, rankings):
        """Calculate the novelty of the recommendations."""
        n = 0
        total = 0

        for userID in topNPredicted.keys():
            for courseID, _ in topNPredicted[userID]:
                rank = rankings[courseID]
                total += rank
                n += 1

        return total / n
