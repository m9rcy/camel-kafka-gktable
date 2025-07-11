package com.example.service.predicate;

import com.example.model.Code;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Objects;
import java.util.function.Predicate;

@Service
@Slf4j
public class CodePredicates {
    
    public static Predicate<Code> hasId(String id) {
        return code -> Objects.equals(code.getId(), id);
    }
    
    public static Predicate<Code> hasName(String name) {
        return code -> Objects.equals(code.getName(), name);
    }
    
    public static Predicate<Code> nameContains(String substring) {
        return code -> code.getName() != null && 
                      code.getName().toLowerCase().contains(substring.toLowerCase());
    }
    
    public static Predicate<Code> hasDescription(String description) {
        return code -> Objects.equals(code.getDescription(), description);
    }
    
    public static Predicate<Code> descriptionContains(String substring) {
        return code -> code.getDescription() != null && 
                      code.getDescription().toLowerCase().contains(substring.toLowerCase());
    }
    
    public static Predicate<Code> nameStartsWith(String prefix) {
        return code -> code.getName() != null && 
                      code.getName().toLowerCase().startsWith(prefix.toLowerCase());
    }
    
    public static Predicate<Code> nameEndsWith(String suffix) {
        return code -> code.getName() != null && 
                      code.getName().toLowerCase().endsWith(suffix.toLowerCase());
    }
    
    public static Predicate<Code> hasEmptyDescription() {
        return code -> code.getDescription() == null || code.getDescription().trim().isEmpty();
    }
    
    public static Predicate<Code> hasNonEmptyDescription() {
        return code -> code.getDescription() != null && !code.getDescription().trim().isEmpty();
    }
    
    public static Predicate<Code> idMatches(String regex) {
        return code -> code.getId() != null && code.getId().matches(regex);
    }
    
    public static Predicate<Code> nameMatches(String regex) {
        return code -> code.getName() != null && code.getName().matches(regex);
    }
    
    // Utility methods for combining predicates
    public static <T> Predicate<T> not(Predicate<T> predicate) {
        return predicate.negate();
    }
    
    public static <T> Predicate<T> and(Predicate<T> first, Predicate<T> second) {
        return first.and(second);
    }
    
    public static <T> Predicate<T> or(Predicate<T> first, Predicate<T> second) {
        return first.or(second);
    }
}