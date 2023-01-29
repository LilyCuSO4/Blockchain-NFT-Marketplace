package com.lilycuso4.ecommerce.dao;

import com.lilycuso4.ecommerce.entity.Product;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ProductRepository extends JpaRepository<Product,Long>{
}
