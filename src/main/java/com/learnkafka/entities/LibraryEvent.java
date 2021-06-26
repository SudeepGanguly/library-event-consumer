package com.learnkafka.entities;

import lombok.*;
import org.jetbrains.annotations.NotNull;

import javax.persistence.*;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@Entity
public class LibraryEvent {

    @Id
    @GeneratedValue
    private Integer libraryEventId;

    @OneToOne(mappedBy="libraryEvent", cascade = {CascadeType.ALL})
    @ToString.Exclude
    private Book book;

    @Enumerated(EnumType.STRING)
    private LibraryEventType eventType;
}
