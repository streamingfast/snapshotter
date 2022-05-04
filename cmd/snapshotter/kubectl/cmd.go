package kubectl

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"strings"
	"time"

	"go.uber.org/zap"
)

func GetPVs() ([]PersistentVolume, error) {
	cmd := exec.Command("kubectl", "get", "pv", "-o", "json")
	zlog.Info("get pv", zap.Stringer("command", cmd))

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	defer stdout.Close()

	err = cmd.Start()
	if err != nil {
		return nil, err
	}

	var output PVOutput
	if err := json.NewDecoder(stdout).Decode(&output); err != nil {
		if err != io.EOF {
			return nil, err
		}
	}

	err = cmd.Wait()
	if err != nil {
		return nil, fmt.Errorf("make sure you are logged in: %w", err)
	}

	return output.Items, nil
}

func GetStatefulSetFromPod(podName string) (string, error) {
	podNameParts := strings.Split(podName, "-")
	stsName := strings.Join(podNameParts[:len(podNameParts)-1], "-")

	return stsName, nil
}

func GetStatefulSetDefinitionFile(stsName string, namespace string) (string, func(), error) {
	cmd := exec.Command("kubectl", "-n", namespace, "get", "sts", stsName, "-o", "json")
	zlog.Info("get sts definition", zap.Stringer("command", cmd))

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return "", func() {}, err
	}
	defer stdout.Close()

	err = cmd.Start()
	if err != nil {
		return "", func() {}, err
	}

	content, err := ioutil.ReadAll(stdout)
	if err != nil {
		return "", func() {}, err
	}

	err = cmd.Wait()
	if err != nil {
		return "", func() {}, fmt.Errorf("make sure you are logged in: %w", err)
	}

	filepath, err := func() (string, error) {
		f, err := ioutil.TempFile("", stsName)
		if err != nil {
			return "", err
		}
		defer f.Close()
		zlog.Debug("created tmp file", zap.String("file", f.Name()))

		_, err = f.Write(content)
		if err != nil {
			return "", err
		}
		return f.Name(), nil
	}()
	if err != nil {
		return "", func() {}, err
	}

	cleanupFunc := func() {
		err := os.Remove(filepath)
		if err != nil {
			zlog.Error("could not remove file", zap.Error(err))
		}
	}

	return filepath, cleanupFunc, nil
}

func DeleteStatefulSet(stsName string, namespace string) error {
	cmd := exec.Command("kubectl", "-n", namespace, "delete", "sts", stsName, "--cascade=false")
	zlog.Info("delete sts", zap.Stringer("command", cmd))

	err := cmd.Start()
	if err != nil {
		return err
	}

	err = cmd.Wait()
	if err != nil {
		return fmt.Errorf("make sure you are logged in: %w", err)
	}

	return nil
}

func DeletePod(podName string, namespace string) error {
	cmd := exec.Command("kubectl", "-n", namespace, "delete", "pod", podName)
	zlog.Info("delete pod", zap.Stringer("command", cmd))

	err := cmd.Start()
	if err != nil {
		return err
	}

	err = cmd.Wait()
	if err != nil {
		return fmt.Errorf("make sure you are logged in: %w", err)
	}

	<-time.After(15 * time.Second)
	return nil
}

func CreateStatefulSetFromFile(filename string) error {
	cmd := exec.Command("kubectl", "apply", "-f", filename)
	zlog.Info("create sts", zap.Stringer("command", cmd))

	err := cmd.Start()
	if err != nil {
		return err
	}

	err = cmd.Wait()
	if err != nil {
		return fmt.Errorf("make sure you are logged in: %w", err)
	}

	return nil
}
