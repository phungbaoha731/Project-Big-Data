package bk.edu.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.util.List;
@AllArgsConstructor
@Getter
@Setter
public class StatsDouble {
    private List<String> players;
    private double score;
}
