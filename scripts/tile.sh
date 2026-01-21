echo "Tiling output: Res 4"
tippecanoe -fo "output/res4.pmtiles" -Z2 -z2 -z2 -B2 \
                -x h3 -l r4agg --no-progress-indicator \
                -P output/h3_4_weekly.geojsonseq

echo "Tiling output: Res 6"
tippecanoe -fo "output/res6.pmtiles" -Z4 -z4 -z4 -B4 \
                -x h3 -l r6agg --no-progress-indicator \
                -P output/h3_6_weekly.geojsonseq

echo "Tiling output: Full res centroids"
tippecanoe -fo "output/res8.pmtiles" -Z6 -z6 -z6 -B6 \
                -x h3 -l daily --no-progress-indicator \
                -P output/daily_editing_per_user.geojsonseq

echo "Tiling output: Full res bounding boxes"
tippecanoe -fo "output/res8_bboxes.pmtiles" -Z6 -z6 -z6 -B6 \
                -x h3 -l daily --no-progress-indicator \
                -P output/daily_bboxes.geojsonseq

rm output/*.geojsonseq