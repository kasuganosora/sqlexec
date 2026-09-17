package memory

import (
	"math"
	"sort"
)

type ivfClusterDist struct {
	clusterID int
	distance  float32
}

// selectProbedClusters returns up to nprobe non-empty cluster IDs ordered by distance.
// Empty clusters are omitted so they cannot crowd out populated ones during sort.
func selectProbedClusters(nlist, nprobe int, clusterCounts []int, distAt func(clusterID int) float32) []int {
	scored := make([]ivfClusterDist, 0, nlist)
	for j := 0; j < nlist; j++ {
		if j < len(clusterCounts) && clusterCounts[j] == 0 {
			continue
		}
		scored = append(scored, ivfClusterDist{
			clusterID: j,
			distance:  distAt(j),
		})
	}
	sort.Slice(scored, func(a, b int) bool {
		return scored[a].distance < scored[b].distance
	})
	if nprobe > len(scored) {
		nprobe = len(scored)
	}
	out := make([]int, nprobe)
	for i := 0; i < nprobe; i++ {
		out[i] = scored[i].clusterID
	}
	return out
}

func nearestCentroid(centroids [][]float32, vec []float32, distFunc DistanceFunc) int {
	best := 0
	minDist := float32(math.MaxFloat32)
	found := false
	for j, center := range centroids {
		if len(center) != len(vec) {
			continue
		}
		d := distFunc.Compute(vec, center)
		if !found || d < minDist {
			minDist = d
			best = j
			found = true
		}
	}
	return best
}

func removeRecordFromCluster(vectorsByCluster map[int][]VectorRecord, clusterCounts []int, clusterID int, id int64) {
	vecs := vectorsByCluster[clusterID]
	for j, rec := range vecs {
		if rec.ID == id {
			vectorsByCluster[clusterID] = append(vecs[:j], vecs[j+1:]...)
			break
		}
	}
	if clusterID >= 0 && clusterID < len(clusterCounts) && clusterCounts[clusterID] > 0 {
		clusterCounts[clusterID]--
	}
}

func pqCodebooksTrained(codebooks [][][]float32) bool {
	return len(codebooks) > 0 && len(codebooks[0]) > 0 && len(codebooks[0][0]) > 0
}
