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

class FundosFiltroForm extends Form {

	protected function _buildSchema(Schema $schema): Schema {
		return $schema->addField('nome', ['type' => 'text'])
						->addField('administrador', ['type' => 'text'])
						->addField('apenasFuncionando', ['type' => 'checkbox'])
		;
	}

	public function validationDefault(Validator $validator): Validator {
		$validator->minLength('nome', 0)
				->minLength('administrador', 0)
				->allowEmptyString('nome')
				->allowEmptyString('administrador')
		//->email('email')
		;
		return $validator;
	}

	protected function _execute(array $data): bool {
		return true;
	}

}
