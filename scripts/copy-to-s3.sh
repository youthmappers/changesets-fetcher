#!/usr/bin/env bash

LATEST_DS=$(cat output/latest_ds.txt)

echo $LATEST_DS

aws --region us-west-2 s3 cp output/res4.pmtiles s3://youthmappers-usw2/activity-dashboard/ds=$LATEST_DS/res4.pmtiles
aws --region us-west-2 s3 cp output/res6.pmtiles s3://youthmappers-usw2/activity-dashboard/ds=$LATEST_DS/res6.pmtiles
aws --region us-west-2 s3 cp output/res8.pmtiles s3://youthmappers-usw2/activity-dashboard/ds=$LATEST_DS/res8.pmtiles
aws --region us-west-2 s3 cp output/res8_bboxes.pmtiles s3://youthmappers-usw2/activity-dashboard/ds=$LATEST_DS/res8_bboxes.pmtiles

aws --region us-west-2 s3 cp output/weekly_chapter_activity.csv s3://youthmappers-usw2/activity-dashboard/ds=$LATEST_DS/weekly_chapter_activity.csv
aws --region us-west-2 s3 cp output/top_edited_countries.json s3://youthmappers-usw2/activity-dashboard/ds=$LATEST_DS/top_edited_countries.json
aws --region us-west-2 s3 cp output/monthly_activity_all_time.json s3://youthmappers-usw2/activity-dashboard/ds=$LATEST_DS/monthly_activity_all_time.json

# The activity file goes at the root of the activity-dashboard folder
aws --region us-west-2 s3 cp output/activity.json s3://youthmappers-usw2/activity-dashboard/activity.json

# The daily rollup goes to the general activity folder
aws --region us-west-2 s3 cp output/daily_rollup.parquet s3://youthmappers-usw2/activity/daily_rollup.parquet

# Finally, clear the cloudfront cache:
aws --region us-west-2 cloudfront create-invalidation --distribution-id E6U9U7HMQT3MF --paths "/activity-dashboard/activity.json"
aws --region us-west-2 cloudfront create-invalidation --distribution-id E6U9U7HMQT3MF --paths "/activity/daily_rollup.parquet"
