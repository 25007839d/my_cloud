resource "google_compute_subnetwork" "vps-test1-subnet1"{
                name = "vps-test1-subnet1"
                ip_cidr_range = "10.10.1.0/27"
                network = "projects/dushyant-373205/global/networks/vps-test1"


}