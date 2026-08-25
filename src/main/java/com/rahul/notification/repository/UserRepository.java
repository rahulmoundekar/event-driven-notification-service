package com.rahul.notification.repository;

import com.rahul.notification.entity.User;
import org.springframework.data.jpa.repository.JpaRepository;

public interface UserRepository
        extends JpaRepository<User, String> {
}