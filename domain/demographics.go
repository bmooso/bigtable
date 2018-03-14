package domain

type (
	PersonalInfo struct {
		ID          string
		FirstName   string
		MiddleName  string
		LastName    string
		Age         string
		ContactInfo ContactInfo
	}

	ContactInfo struct {
		Addresses    []Address
		PhoneNumbers []PhoneNumber
	}

	Address struct {
		StreetName string
		City       string
		State      string
		Zipcode    string
	}

	PhoneNumber struct {
		AreaCode  string
		Number    string
		Extension string
		Type      string
	}
)
