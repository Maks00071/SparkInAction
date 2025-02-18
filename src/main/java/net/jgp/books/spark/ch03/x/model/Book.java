package net.jgp.books.spark.ch03.x.model;

import java.util.Date;


public class Book {
    Integer id;
    Integer authorId;
    String title;
    Date releaseDate;
    String link;


    /**
     * @return the id
     */
    public Integer getId() {
        return id;
    }

    /**
     * The id to set
     *
     * @param id
     */
    public void setId(Integer id) {
        this.id = id;
    }

    /**
     * @return the authorId
     */
    public Integer getAuthorId() {
        return authorId;
    }

    /**
     * The authorId to set
     *
     * @param authorId
     */
//    public void setAuthorId(Integer authorId) {
//        this.authorId = authorId;
//    }

    /**
     * The authorId to set
     *
     * @param authorId
     */
    public void setAuthorId(Integer authorId) {
        if (authorId == null) {
            this.authorId = 0;
        } else {
            this.authorId = authorId;
        }
    }

    /**
     * @return the title
     */
    public String getTitle() {
        return title;
    }

    /**
     * The title to set
     *
     * @param title
     */
    public void setTitle(String title) {
        this.title = title;
    }

    /**
     * @return the releaseDate
     */
    public Date getReleaseDate() {
        return releaseDate;
    }

    /**
     * The releaseDate to set
     *
     * @param releaseDate
     */
    public void setReleaseDate(Date releaseDate) {
        this.releaseDate = releaseDate;
    }

    /**
     * @return the link
     */
    public String getLink() {
        return link;
    }

    /**
     * The link to set
     *
     * @param link
     */
    public void setLink(String link) {
        this.link = link;
    }
}




































