class ProductReviewStatistic:
    def __init__(self, productId):
        self.productId = productId
        self.mean_review = 0
        self.mode_review = 0
        self.median_review = 0
        self.num_reviews = 0

    def get_value(self):
        return self.value

    def increment_average(self, new_review):
        number_of_reviews = self.num_reviews + 1
        current_average = self.mean_review
        self.mean_review = (new_review - current_average) / float(number_of_reviews)
        self.num_reviews += 1
        return self.mean_review

