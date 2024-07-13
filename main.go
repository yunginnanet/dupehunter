package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"image"
	_ "image/gif"
	_ "image/jpeg"
	_ "image/png"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"git.tcp.direct/kayos/common/pool"
	"git.tcp.direct/tcp.direct/database"
	"git.tcp.direct/tcp.direct/database/loader"
	"git.tcp.direct/tcp.direct/database/pogreb"
	"git.tcp.direct/tcp.direct/database/registry"
	"github.com/bytedance/sonic"
	"github.com/bytedance/sonic/encoder"
	"github.com/corona10/goimagehash"
	"github.com/rs/zerolog"

	"github.com/panjf2000/ants/v2"
)

var (
	DB         database.Keeper
	log        zerolog.Logger
	Collection []*Image
	workers    *ants.Pool
	bufs       = pool.NewBufferFactory()
)

type ImageType uint8

func (it ImageType) String() string {
	if s, ok := imageTypeToString[it]; ok {
		return s
	}
	return NULL.String()
}

var ErrUnknownImageType = errors.New("unknown image type")

const (
	NULL ImageType = iota
	JPEG
	PNG
	GIF
)

var imageTypeToString = map[ImageType]string{
	NULL: "null",
	JPEG: "jpeg",
	PNG:  "png",
	GIF:  "gif",
}

var stringToImageType = map[string]ImageType{
	"null": NULL,
	"jpeg": JPEG,
	"png":  PNG,
	"gif":  GIF,
}

func parseImageType(s string) (ImageType, error) {
	s = strings.ToLower(s)
	if val, ok := stringToImageType[s]; ok {
		return val, nil
	}
	return NULL, ErrUnknownImageType
}

type Image struct {
	Type    ImageType
	Name    string
	Path    string
	ModTime time.Time
	Size    int64
	PHash   []byte

	fin       chan struct{}
	closeOnce *sync.Once
	b         *pool.Buffer
	f         *os.File
	i         image.Image
}

func init() {
	log = zerolog.New(zerolog.ConsoleWriter{Out: os.Stdout, NoColor: false}).With().Timestamp().Logger()
	zerolog.SetGlobalLevel(zerolog.DebugLevel)
	startDatastore()
	startWorkerPool()
}

func startDatastore() {
	home, err := os.UserHomeDir()
	if err != nil {
		log.Fatal().Err(err).Msg("failed to determine home directory")
	}
	dest := filepath.Join(home, ".local/share/dupehunter/db")
	destStat, statErr := os.Stat(dest)
	if errors.Is(statErr, os.ErrNotExist) {
		if err = os.MkdirAll(dest, 0755); err != nil {
			log.Fatal().Caller().Msg(err.Error())
		}
		if destStat, statErr = os.Stat(dest); statErr != nil || destStat == nil {
			if statErr == nil {
				statErr = errors.New("tried to make target directory, but it's still missing")
			}
			log.Fatal().Caller().Msg(statErr.Error())
		}
	}
	if destStat == nil {
		log.Fatal().Caller().Msg("failed to stat target directory")
	}
	log.Trace().Interface("stat", destStat.Sys()).Msg("opening database...")
	if DB, err = loader.OpenKeeper(dest, &pogreb.WrappedOptions{AllowRecovery: true}); errors.Is(err, os.ErrNotExist) {
		log.Info().Str("path", dest).Msg("creating new database...")
		if DB, err = registry.GetKeeper("pogreb")(dest, &pogreb.WrappedOptions{AllowRecovery: true}); err != nil || DB == nil {
			if err == nil {
				err = errors.New("nil keeper")
			}
			log.Fatal().Caller().Msg(err.Error())
		}
	}
	if DB == nil {
		log.Fatal().Err(err).Caller().Msg("failed to open database")
	}
	if //goland:noinspection GoNilness
	err = DB.Init("images", &pogreb.WrappedOptions{AllowRecovery: true}); err != nil &&
		!errors.Is(err, pogreb.ErrStoreExists) {
		log.Panic().Caller().Msg(err.Error())
	}
}

func startWorkerPool() {
	var poolErr error
	if workers, poolErr = ants.NewPool(25,
		ants.WithPanicHandler(func(i interface{}) {
			log.Error().Caller(1).Stack().Interface("panic", i).Msg("Worker panic!")
		}),
		ants.WithLogger(&log),
	); poolErr != nil {
		log.Fatal().Caller().Str("caller", "worker pool").Msg(poolErr.Error())
	}
}

func NewImage(path string, finChan chan struct{}) (*Image, error) {
	path, _ = filepath.Abs(path)
	finfo, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if finfo.IsDir() {
		return nil, errors.New("target is a directory: " + path)
	}
	i := &Image{
		Path:      path,
		Name:      finfo.Name(),
		Type:      NULL,
		ModTime:   finfo.ModTime(),
		Size:      finfo.Size(),
		closeOnce: &sync.Once{},
		b:         bufs.Get(),
		fin:       finChan,
	}

	if CheckExisting(i, DB.With("images")) {
		return nil, errors.New("file already in database: " + i.Name)
	}

	return i, nil
}

var ErrAlreadyClosed = errors.New("image already closed")

func (img *Image) Close() error {
	closedTwice := ErrAlreadyClosed
	img.closeOnce.Do(func() {
		bufs.MustPut(img.b)
		close(img.fin)
		closedTwice = nil
	})
	img.b = nil
	return closedTwice
}

func (img *Image) Read(p []byte) (n int, err error) {
	n = copy(p, img.PHash)
	if n != len(img.PHash) {
		return n, io.ErrShortBuffer
	}
	return len(p), nil
}

func CheckExisting(img *Image, db database.Filer) (ok bool) {
	if has := db.Has([]byte(img.Path)); !has {
		return false
	}
	existing, dbErr := db.Get([]byte(img.Path))
	if dbErr != nil {
		log.Error().Err(dbErr).Caller().Msg("database error")
		return true
	}
	var recall Image
	if jErr := sonic.Unmarshal(existing, &recall); jErr != nil {
		log.Error().Err(jErr).Caller().Msg("unmarshal error")
		return true
	}
	if recall.ModTime == img.ModTime && recall.Size == img.Size {
		return true
	}
	return false
}

func processFile(img *Image) (err error) {
	var f *os.File
	f, err = os.Open(img.Path)
	if err != nil {
		return err
	}
	img.f = f
	return err
}

func decImg(r io.Reader) (image.Image, ImageType, error) {
	img, t, e := image.Decode(r)
	if e != nil {
		return nil, NULL, e
	}
	var it ImageType
	var itErr error
	if it, itErr = parseImageType(t); itErr != nil {
		return nil, NULL, itErr
	}
	return img, it, nil
}

func (img *Image) decodeImage() (err error) {
	defer func() {
		if closeErr := img.f.Close(); closeErr != nil {
			errs := []error{err, closeErr}
			err = errors.Join(errs...)
		}
	}()
	img.i, img.Type, err = decImg(img.f)
	return err
}

func ingestImage(img *Image) error {
	if img == nil {
		return errors.New("not an image")
	}

	phash, hashErr := goimagehash.DifferenceHash(img.i)
	if hashErr != nil {
		return hashErr
	}
	dumpErr := phash.Dump(img.b)
	if dumpErr != nil {
		return dumpErr
	}
	img.PHash = make([]byte, img.b.Len())
	n, rErr := img.b.Read(img.PHash)
	if (n == 0 || n < img.b.Len()) && rErr == nil {
		rErr = io.ErrShortWrite
	}
	if rErr != nil {
		return rErr
	}
	_ = img.b.Reset()
	if err := encoder.NewStreamEncoder(img.b).Encode(&img); err != nil {
		return fmt.Errorf("json encoder: %w", err)
	}

	if err := DB.With("images").
		Put([]byte(img.Path), bytes.TrimSuffix(img.b.Bytes(), []byte("\n"))); err != nil {
		return err
	}

	log.Info().Str("caller", img.Name).RawJSON("data", img.b.Bytes()).Msg("done!")

	return nil
}

func (img *Image) FinalProcessing() {
	err := img.decodeImage()
	if err != nil {
		log.Warn().Caller().Err(err).Str("caller", img.Name).Msg("failed to ingest")
		img.fin <- struct{}{}
		return
	}
	if img.Type == NULL {
		log.Trace().Caller().Str("caller", img.Name).Msg("skipping null imagetype")
		img.fin <- struct{}{}
		return
	}
	err = ingestImage(img)
	if err != nil {
		log.Debug().Caller().Str("caller", img.Name).Msg("failed to ingest: " + err.Error())
		img.fin <- struct{}{}
		return
	}
	Collection = append(Collection, img)
	img.fin <- struct{}{}
}

func process(filePath string, finChan chan struct{}) {
	log.Debug().Msgf("processing: %s", filePath)
	img, err := NewImage(filePath, finChan)
	if err != nil {
		log.Warn().Caller().Str("caller", filePath).Msg(err.Error())
		finChan <- struct{}{}
		return
	}
	err = processFile(img)
	if err != nil {
		log.Warn().Caller().Str("caller", filePath).Msg(err.Error())
		finChan <- struct{}{}
		return
	}
	err = workers.Submit(img.FinalProcessing)
	if err != nil {
		log.Fatal().Msg(err.Error())
	}
}

func processArgs(args []string) {
	var processed = 0
	var finChan = make(chan struct{})
	for i, arg := range args {
		if i == 0 {
			continue
		}
		go process(arg, finChan)
	}
mainLoop:
	for {
		select {
		case <-finChan:
			processed++
		default:
			if processed >= len(args)-1 {
				if processed > 0 {
					log.Info().Int("processed", processed).Msg("finished")
				}
				break mainLoop
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	_ = DB.SyncAll()
}

func checkAll(maxDistance int) error {
	var (
		images     = make(map[string]*goimagehash.ImageHash)
		dupesFound = make(map[string]struct{})
	)

	for _, k := range DB.With("images").Keys() {
		dat, err := DB.With("images").Get(k)
		if err != nil {
			log.Fatal().Err(err).Send()
		}
		log.Trace().Msgf("%s: %s", string(k), string(dat))
		i := &Image{}
		if err = sonic.Unmarshal(dat, i); err != nil {
			return fmt.Errorf("json deserialize fail: %w", err)
		}
		dhash, err := goimagehash.LoadImageHash(i)
		if err != nil {
			return fmt.Errorf("failed to load image hash for %s: %w", i.Path, err)
		}
		images[i.Path] = dhash
	}

	for k, v := range images {
		for l, b := range images {
			if l == k {
				continue
			}
			if _, ok := dupesFound[l]; ok {
				continue
			}
			distance, err := v.Distance(b)
			if err != nil {
				return fmt.Errorf("failed to calculate distance between %s and %s: %w", k, l, err)
			}
			log.Trace().Msgf("%s vs %s: %d", k, l, distance)
			if distance < maxDistance {
				log.Info().Msgf("duplicate found: %s and %s", k, l)
				dupesFound[k] = struct{}{}
				dupesFound[l] = struct{}{}
			}
		}
	}

	return nil
}

func processStdin() []string {
	var args = make([]string, 0)
	bufIn := bufio.NewScanner(os.Stdin)
	for bufIn.Scan() {
		args = append(args, bufIn.Text())
	}
	return args
}

func main() {
	var maxDistance = 12

	for i, arg := range os.Args {
		if arg == "-d" && len(os.Args)+1 > i {
			mdInt, intErr := strconv.Atoi(os.Args[i+1])
			if intErr != nil {
				log.Fatal().Err(intErr).Msgf("failed to parse max distance %s", os.Args[i+1])
			}
			maxDistance = mdInt
			os.Args = append(os.Args[:i], os.Args[i+2:]...)
			continue
		}
		if arg == "-v" {
			zerolog.SetGlobalLevel(zerolog.TraceLevel)
			os.Args = append(os.Args[:i], os.Args[i+1:]...)
		}
	}

	if len(os.Args) == 2 && os.Args[1] == "-" {
		processArgs(processStdin())
	}

	if len(os.Args) > 0 {
		processArgs(os.Args)
	}

	if err := checkAll(maxDistance); err != nil {
		log.Fatal().Err(err).Send()
	}

	if err := DB.SyncAndCloseAll(); err != nil {
		log.Fatal().Err(err).Msg("failed to sync and close all databases")
	}
}
