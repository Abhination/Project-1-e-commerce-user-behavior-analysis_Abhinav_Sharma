from mrjob.job import MRJob
from mrjob.step import MRStep
import csv

class RevenueProfitabilityAnalysis(MRJob):
    
    def configure_args(self):
        super(RevenueProfitabilityAnalysis, self).configure_args()
        self.add_file_arg('--products', help="Path to the products.csv file")
    
    def load_products(self):
        self.products = {}
        with open(self.options.products, 'r') as f:
            reader = csv.reader(f)
            next(reader)
            for row in reader:
                self.products[row[0]] = (row[1], row[2])
    
    def steps(self):
        return [
            MRStep(mapper_init=self.load_products,
                   mapper=self.mapper_revenue,
                   reducer=self.reducer_aggregate_revenue),
            MRStep(reducer=self.reducer_calculate_average)
        ]
    
    def mapper_revenue(self, _, line):
        fields = line.split(',')
        if fields[0] != 'TransactionID':  # Skip the header row
            product_id = fields[3]
            revenue = float(fields[5])
            if product_id in self.products:
                product_name, category = self.products[product_id]
                yield (product_id, product_name, category), revenue
    
    def reducer_aggregate_revenue(self, product_info, revenues):
        total_revenue = sum(revenues)
        yield product_info[2], (total_revenue, product_info[1], product_info[0])
    
    def reducer_calculate_average(self, category, product_info_list):
        top_products = sorted(product_info_list, reverse=True)[:3]
        for total_revenue, product_name, product_id in top_products:
            yield category, (product_name, total_revenue)

if __name__ == '__main__':
    RevenueProfitabilityAnalysis.run()
