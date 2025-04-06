from pyspark.sql.functions import udf, col, lit
from pyspark.sql.types import ArrayType, StringType

# Define a UDF to extract top features from TF-IDF
def get_top_features_udf(vocab, top_n=15):
    def extract(features):
        indices = features.indices
        values = features.values
        sorted_indices = sorted(zip(indices, values), key=lambda x: -x[1])[:top_n]
        return [vocab[i] for i, _ in sorted_indices]
    return udf(extract, ArrayType(StringType()))

# Use the vocabulary from the TF-IDF process (assuming it's available)
vocab = vectorizer_model.vocabulary  # Vocabulary from your CountVectorizer

# Add top words for each cluster
top_features_udf = get_top_features_udf(vocab)

df_clusters_with_top_features = df_tfidf.groupBy("prediction").agg(
    F.first("cuisine").alias("cuisine"),
    top_features_udf(F.first("tfidf_features")).alias("top_words"),
    F.count("*").alias("document_count")
)

# Add percentages to the report
total_documents = df_tfidf.count()

df_report = df_clusters_with_top_features.withColumn(
    "percentage",
    (col("document_count") / total_documents * 100).alias("percentage")
)

# Show the final report
df_report.show(truncate=False)
