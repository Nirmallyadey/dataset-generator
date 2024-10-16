# Install required packages if not already installed
install.packages("data.table")  # For efficient data handling and writing

# Load required libraries
library(data.table)

# Set the number of total rows and columns
n_rows <- 1.1e9  # 1.1 billion rows
n_cols <- 12     # 12 columns

# Function to generate random data for a chunk of rows
generate_data <- function(n_rows, n_cols) {
  
  # Create a data.table with random values for columns col1 to col12
  dt <- data.table(
    col1 = sample(1:100, n_rows, replace = TRUE),        # Random integers
    col2 = sample(letters, n_rows, replace = TRUE),      # Random letters
    col3 = runif(n_rows, min = 0, max = 100),            # Random floats
    col4 = sample(c(TRUE, FALSE), n_rows, replace = TRUE), # Random TRUE/FALSE
    col5 = sample(1:100, n_rows, replace = TRUE),        # This column will have garbage values later
    col6 = sample(letters, n_rows, replace = TRUE),
    col7 = runif(n_rows, min = 0, max = 100),
    col8 = sample(1:100, n_rows, replace = TRUE),
    col9 = sample(letters, n_rows, replace = TRUE),
    col10 = runif(n_rows, min = 0, max = 100),
    col11 = sample(c(TRUE, FALSE), n_rows, replace = TRUE),
    col12 = sample(letters, n_rows, replace = TRUE)
  )
  
  # Introduce garbage, null, undefined values into col5
  # Insert NA (null values), empty strings, "junk", and "undefined"
  dt$col5[sample(1:n_rows, n_rows * 0.1)] <- NA          # 10% NAs
  dt$col5[sample(1:n_rows, n_rows * 0.05)] <- ""         # 5% empty strings
  dt$col5[sample(1:n_rows, n_rows * 0.05)] <- "junk"     # 5% garbage values
  dt$col5[sample(1:n_rows, n_rows * 0.05)] <- "undefined" # 5% 'undefined' values
  
  return(dt)
}

# Number of rows per batch (adjust based on your system memory, e.g., 10 million rows)
batch_size <- 1e7  # 10 million rows

# Output file path
output_file <- "large_dataset.csv"

# Initialize CSV by writing the header
fwrite(data.table(col1 = integer(), col2 = character(), col3 = numeric(),
                  col4 = logical(), col5 = character(), col6 = character(),
                  col7 = numeric(), col8 = integer(), col9 = character(),
                  col10 = numeric(), col11 = logical(), col12 = character()), 
       file = output_file, append = FALSE)

# Generate and write data in batches
system.time({
  for (i in 1:ceiling(n_rows / batch_size)) {
    # Calculate the number of rows for the current batch (in case it's the last smaller batch)
    current_batch_size <- min(batch_size, n_rows - (i - 1) * batch_size)
    
    # Generate the current batch of data
    batch_data <- generate_data(current_batch_size, n_cols)
    
    # Append the batch to the CSV file
    fwrite(batch_data, file = output_file, append = TRUE)
    
    # Print progress
    print(paste("Batch", i, "of", ceiling(n_rows / batch_size), "written."))
  }
})

# Confirm completion
print("Data generation complete. CSV file is ready.")
