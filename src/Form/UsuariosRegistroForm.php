<?php

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

namespace App\Form;

use Cake\Form\Form;
use Cake\Form\Schema;
use Cake\Validation\Validator;
use App\Controller;

class UsuariosRegistroForm extends Form {

	protected function _buildSchema(Schema $schema): Schema {
		$schema->addField('cpf', ['type' => 'number'])
				->addField('nome', ['type' => 'string'])
				->addField('email', ['type' => 'email'])
				->addField('dt_nasc', ['type' => 'date'])
				->addField('senha', ['type' => 'password'])
				->addField('concordaTermos', ['type' => 'boolean'])
		;
		return $schema;
	}

	public function validationDefault(Validator $validator): Validator {
		return $validator;
	}

	protected function _execute(array $data): bool {
		// Send an email.
		return true;
	}

}
