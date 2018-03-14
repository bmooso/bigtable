package internal

import (
	"encoding/json"
	"net/http"

	"github.com/bmooso/bigtable/domain"
	"github.com/bmooso/bigtable/store"
	"github.com/labstack/echo"
)

type (
	SubscriberHandler struct {
		RowMetaData  store.RowMetaData
		MessageStore store.MessageStore
	}

	Error struct {
		Message string `json:"error"`
	}
)

const (
	subscriberCN = "PersonalInfo"
)

func (sh SubscriberHandler) InitRoutes(g *echo.Group) {
	g.GET("/:id", sh.read)
	g.GET("", sh.readAll)
	g.POST("", sh.create)
	g.PUT("/:id", sh.update)
	g.DELETE("/:id", sh.delete)
}

func (sh SubscriberHandler) create(c echo.Context) error {
	var pi domain.PersonalInfo
	if err := c.Bind(&pi); err != nil {
		return err
	}

	return sh.MessageStore.CreateNew(sh.RowMetaData, pi)
}

func (sh SubscriberHandler) read(c echo.Context) error {
	id := c.Param("id")
	return sh.MessageStore.ReadSingle(sh.RowMetaData, subscriberCN, id)
}

func (sh SubscriberHandler) readAll(c echo.Context) error {

	results, err := sh.MessageStore.ReadAll(sh.RowMetaData, subscriberCN)

	if err != nil {
		return c.JSON(http.StatusBadRequest, toJSON(err))
	}

	s := make([]domain.PersonalInfo, len(results))

	i := 0

	for key, val := range results {
		var pi domain.PersonalInfo
		err := json.Unmarshal(val, &pi)
		if err != nil {
			return c.JSON(http.StatusBadRequest, toJSON(err))
		}
		pi.ID = key
		s[i] = pi
		i++
	}

	return c.JSON(http.StatusOK, s)
}

func (sh SubscriberHandler) readAllDeleted(c echo.Context) error {

	return sh.MessageStore.ReadAllDeleted(sh.RowMetaData, subscriberCN)
}

func (sh SubscriberHandler) update(c echo.Context) error {
	var pi domain.PersonalInfo
	if err := c.Bind(&pi); err != nil {
		return err
	}

	id := c.Param("id")
	return sh.MessageStore.Update(sh.RowMetaData, id, pi)
}

func (sh SubscriberHandler) delete(c echo.Context) error {
	id := c.Param("id")
	return sh.MessageStore.Delete(sh.RowMetaData, subscriberCN, id)

}

func toJSON(err error) Error {
	return Error{Message: err.Error()}
}
