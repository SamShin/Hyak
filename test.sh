dir_path="/mmfs1/home/seunguk"

# Get the owner of the directory
owner=$(ls -ld $dir_path | awk '{print $3}')

# Get the total size of the directory
size=$(du -sh $dir_path | awk '{print $1}')

# Display the results
echo "Owner: $owner"
echo "Total Size: $size"
