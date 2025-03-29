package net.jgp.books.spark.ch09.x.extlib;

import java.io.Serializable;
import java.io.File;
import java.util.List;
import java.util.Arrays;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Обобщенный и многократно используемый "наблюдатель" за файлами с поддержкой
 * фильтрации по расширениям, порогового значения (максимального количества
 * обрабатываемых файлов) и необязательной рекурсии.
 * Предназначен не только для обработки файлов фотографий.
 */
public class RecursiveExtensionFilteredLister implements Serializable {

    @SuppressWarnings("unused")  // отключение предупреждений
    private static transient Logger log = LoggerFactory
            .getLogger(RecursiveExtensionFilteredLister.class);

    private static final long serialVersionUID = -5751014237854623589L;

    private List<String> extensions;
    private List<File> files;
    private int limit;
    private boolean recursive;
    private boolean hasChanged;
    private String startPath = null;

    /**
     * Constructor, sets the default values
     */
    public RecursiveExtensionFilteredLister() {
        this.files = new ArrayList<>();
        this.extensions = new ArrayList<>();
        this.recursive = false;
        this.limit = -1;
        this.hasChanged = true;
    }

    /**
     * Adds an extension to filter. Can start with a dot or not. Is case
     * sensitive: .jpg is not the same as .JPG
     *
     * @param extension to filter on
     */
    public void addExtension(String extension) {
        if (extension.startsWith(".")) {
            this.extensions.add(extension);
        } else {
            this.extensions.add("." + extension);
        }
        this.hasChanged = true;
    }

    /**
     * Checks if the file is matching our constraints
     *
     * @param dir
     * @param name
     * @return true/false
     */
    private boolean check(File dir, String name) {
        File f = new File(dir, name);
        if (f.isDirectory()) {
            if (recursive) {
                listO(f);
            }
            return false;
        } else {
            for (String ext : extensions) {
                if (name.toLowerCase().endsWith(ext)) {
                    return true;
                }
            }
            return false;
        }
    }

    /**
     * @return
     */
    private boolean dir() {
        if (this.startPath == null) {
            return false;
        }
        return listO(new File(this.startPath));
    }

    /**
     * Returns a list of files based on the criteria given by setters (or
     * default values)
     *
     * @return a list of files
     */
    public List<File> getFiles() {
        if (this.hasChanged == true) {
            dir();
            this.hasChanged = false;
        }
        return files;
    }

    /**
     * Метод рекурсивного обхода директорий
     *
     * @param folder
     * @return true/false
     */
    private boolean listO(File folder) {
        if (folder == null) {
            return false;
        }
        if (!folder.isDirectory()) {
            return false;
        }

        File[] listOfFiles = folder.listFiles((dir, name) -> check(dir, name));
        if (listOfFiles == null) {
            return true;
        }
        if (limit == -1) {
            this.files.addAll(Arrays.asList(listOfFiles));
        } else {
            int fileCount = this.files.size();
            if (fileCount >= limit) {
                recursive = false;
                return false;
            }

            for (int i = fileCount, j = 0; i < limit && j < listOfFiles.length; i++, j++) {
                this.files.add(listOfFiles[j]);
            }
        }
        return true;
    }

    /**
     * Sets the limit of files you want to list. Default is no limit.
     *
     * @param limit
     */
    public void setLimit(int limit) {
        this.limit = limit;
        this.hasChanged = true;
    }

    /**
     * Sets the starting path
     *
     * @param newPath
     */
    public void setPath(String newPath) {
        this.startPath = newPath;
        this.hasChanged = true;
    }

    /**
     * Sets the recursion, default is false
     *
     * @param recursive
     */
    public void setRecursive(boolean recursive) {
        this.recursive = recursive;
        this.hasChanged = true;
    }
}

















































