class ProductReviewStatistic:
    def __init__(self, productId):
        self.productId = productId
        self.mean_review = 0
        self.mode_review = 0
        self.median_review = 0
        self.num_reviews = 0
        self.statistics = [0, 0, 0, 0, 0]
    
    def get_mean(self):
        return self.mean_review

    def get_productId(self):
        return self.productId

    def increment_average(self, new_review):
        number_of_reviews = self.num_reviews + 1
        current_average = self.mean_review
        self.mean_review = (new_review - current_average) / float(number_of_reviews)
        self.mean_review = round(self.mean_review, 1)
        self.num_reviews += 1
        return self.mean_review

    def update_median(self, new_review):
        self.statistics[new_review - 1] += 1

    def is_high_review(self):
        return (self.mean_review >= 4)

    def get_details(self):
        mode = self.statistics.index(max(self.statistics))
        return "Product ID: " + self.productId + "\nMean: " + str(self.mean_review) + "\nMode: " + str(mode) + "\n"


